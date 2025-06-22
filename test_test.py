import os
import logging
import random
import time
import threading
from typing import Any, List, Tuple
import unittest
import queue
import base64

from porcupine.model import Operation
from porcupine.porcupine import check_operations_verbose
from models.kv import KvInput, KvOutput, KvModel
from config import make_single_config, make_shard_config, Config



linearizability_check_timeout = 1  # in seconds
MiB = 1024 * 1024

class OpLog:
    def __init__(self):
        self.operations = []
        self.lock = threading.Lock()

    def append(self, op: Operation):
        with self.lock:
            self.operations.append(op)

    def read(self) -> List[Operation]:
        with self.lock:
            return list(self.operations)

# to make sure timestamps use the monotonic clock, we measure time relative to t0
t0 = time.monotonic()

# get/put/putappend that keep counts
def get(cfg, ck, key: str, log: OpLog, cli: int) -> str:
    start = int((time.monotonic() - t0) * 1e9)
    v = ck.get(key)
    end = int((time.monotonic() - t0) * 1e9)
    cfg.op()
    if log:
        log.append(Operation(
            input=KvInput(op=0, key=key),
            output=KvOutput(value=v),
            call_time=start,
            response_time=end,
            client_id=cli
        ))
    return v

def put(cfg, ck, key: str, value: str, log: OpLog, cli: int):
    start = int((time.monotonic() - t0) * 1e9)
    ck.put(key, value)
    end = int((time.monotonic() - t0) * 1e9)
    cfg.op()
    if log:
        log.append(Operation(
            input=KvInput(op=1, key=key, value=value),
            output=KvOutput(),
            call_time=start,
            response_time=end,
            client_id=cli
        ))

def append(cfg, ck, key: str, value: str, log: OpLog, cli: int) -> str:
    start = int((time.monotonic() - t0) * 1e9)
    last = ck.append(key, value)
    end = int((time.monotonic() - t0) * 1e9)
    cfg.op()
    if log:
        log.append(Operation(
            input=KvInput(op=3, key=key, value=value),
            output=KvOutput(value=last if last is not None else ""),
            call_time=start,
            response_time=end,
            client_id=cli
        ))

    return last

# a client runs the function f and then signals it is done
def run_client(t: unittest.TestCase, cfg, me: int, ca, fn):
    # print(f"client {me} running")
    ok = False
    try:
        ck = cfg.make_client()
        fn(me, ck, t)
        ok = True
        cfg.delete_client(ck)
    finally:
        ca.put(ok)

# spawn ncli clients and wait until they are all done
def spawn_clients_and_wait(t: unittest.TestCase, cfg, ncli: int, fn):
    ca = [None] * ncli
    for cli in range(ncli):
        ca[cli] = queue.Queue()
        threading.Thread(target=run_client, args=(t, cfg, cli, ca[cli], fn,)).start()
    print("spawn_clients_and_wait: waiting for clients")
    for cli in range(ncli):
        ok = ca[cli].get()
        print(f"spawn_clients_and_wait: client {cli} is done")
        if not ok:
            t.fail("failure")

# predict effect of append(k, val) if old value is prev
def next_value(prev: str, val: str) -> str:
    return prev + val

# check that for a specific client all known appends are present in a value,
# and in order
def check_clnt_appends(t: unittest.TestCase, clnt: int, v: str, count: int):
    lastoff = -1
    for j in range(count):
        wanted = f"x {clnt} {j} y"
        off = v.find(wanted)
        if off < 0:
            t.fail(f"{clnt} missing element {wanted} in append result {v}")
        off1 = v.rfind(wanted)
        if off1 != off:
            t.fail(f"duplicate element {wanted} in append result")
        if off <= lastoff:
            t.fail(f"wrong order for element {wanted} in append result")
        lastoff = off

# check that all known appends are present in a value,
# and are in order for each concurrent client
def check_concurrent_appends(t: unittest.TestCase, v: str, counts: List[int]):
    nclients = len(counts)
    for i in range(nclients):
        lastoff = -1
        for j in range(counts[i]):
            wanted = f"x {i} {j} y"
            off = v.find(wanted)
            if off < 0:
                t.fail(f"{i} missing element {wanted} in append result {v}")
            off1 = v.rfind(wanted)
            if off1 != off:
                t.fail(f"duplicate element {wanted} in append result")
            if off <= lastoff:
                t.fail(f"wrong order for element {wanted} in append result")
            lastoff = off

# is ov in nv?
def in_history(value, lst):
    if lst is None:
        return False
    for ov in lst:
        if ov == value:
            return True
    return False


def rand_value(n: int) -> str:
    letter_bytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return ''.join(random.choice(letter_bytes) for _ in range(n))

# Basic test is as follows: one or more clients submitting Append/Get
# operations to the server for some period of time.  After the period
# is over, test checks that all appended values are present and in
# order for a particular key.  If unreliable is set, RPCs may fail.
def generic_test(t: unittest.TestCase, nclients: int, shards: Tuple[int, int], unreliable: bool, randomkeys: bool):
    NITER = 3
    TIME = 1  # in seconds

    title = "Test: "
    if unreliable:
        title += "unreliable net, "
    if randomkeys:
        title += "random keys, "
    if shards[0] > 1:
        title += "sharded, "
    if nclients > 1:
        title += "many clients"
    else:
        title += "one client"

    if shards[0] == 1:
        cfg = make_single_config(t, unreliable)
    else:
        cfg = make_shard_config(t, shards[0], shards[1], unreliable)
    try:
        cfg.begin(title)
        op_log = OpLog()

        ck = cfg.make_client()

        done_clients = threading.Event()
        clnts = [queue.Queue() for _ in range(nclients)]

        for i in range(NITER):
            print(f"Iteration {i}")
            done_clients.clear()

            def spawn_func():

                def client_func(cli, myck, t):
                    print(f"Client {cli}")
                    j = 0
                    try:
                        last = ""  # only used when not randomkeys
                        if not randomkeys:
                            put(cfg, myck, str(cli), last, op_log, cli)
                        while not done_clients.is_set():
                            if randomkeys:
                                key = str(random.randint(0, nclients - 1))
                            else:
                                key = str(cli)
                            nv = f"x {cli} {j} y"
                            if random.randint(0, 1000) < 500:
                                #print(f"{cli}: client new append {nv}")
                                l = append(cfg, myck, key, nv, op_log, cli)
                                if not randomkeys:
                                    if j > 0:
                                        o = f"x {cli} {j-1} y"
                                        if not in_history(o, l):
                                            t.fail(f"error: old {o} not in return\n{l}\n")
                                    if in_history(nv, l):
                                        t.fail(f"error: new value {nv} in returned values\n{l}\n")
                                    last = next_value(last, nv)
                                j += 1
                            elif randomkeys and random.randint(0, 1000) < 100:
                                put(cfg, myck, key, nv, op_log, cli)
                                j += 1
                            else:
                                #print(f"{cli}: client new get {key}")
                                v = get(cfg, myck, key, op_log, cli)
                                if not randomkeys and v != last:
                                    t.fail(f"get wrong value, key {key}, wanted:\n{last}\n, got\n{v}\n")
                    finally:
                        clnts[cli].put(j)

                spawn_clients_and_wait(t, cfg, nclients, client_func)

            threading.Thread(target=spawn_func, args=()).start()

            time.sleep(TIME)

            done_clients.set() # tell clients to quit

            for cli in range(nclients):
                j = clnts[cli].get()
                # if j < 10:
                #     print(f"Warning: client {cli} managed to perform only {j} put operations in 1 sec?\n")
                key = str(cli)
                #print(f"check {j} for client {i}")
                v = get(cfg, ck, key, op_log, 0)
                if not randomkeys:
                    check_clnt_appends(t, cli, v, j)

        res, info = check_operations_verbose(KvModel, op_log.read(), linearizability_check_timeout)
        if res == "Illegal":
            t.fail("history is not linearizable")
        elif res == "Unknown":
            print("info: linearizability check timed out, assuming history is ok")

    finally:
        cfg.cleanup()
        cfg.end()

# Test one client
class TestBasic(unittest.TestCase):
    def test_basic(self):
        generic_test(self, 1, (1, 1), False, False)

# Test many clients
class TestConcurrent(unittest.TestCase):
    def test_concurrent(self):
        generic_test(self, 5, (1, 1), False, False)

# Test: unreliable net, many clients
class TestUnreliable(unittest.TestCase):
    def test_unreliable(self):
        generic_test(self, 5, (1, 1), True, False)

# Test: unreliable net, many clients, one key
class TestUnreliableOneKey(unittest.TestCase):
    def test_unreliable_one_key(self):
        cfg = make_single_config(self, True)
        ck = cfg.make_client()

        cfg.begin("Test: concurrent append to same key, unreliable")

        put(cfg, ck, "k", "", None, -1)

        nclient = 5
        upto = 10

        def client_func(me, myck, t):
            n = 0
            while n < upto:
                nv = f"x {me} {n} y"
                ov = append(cfg, myck, "k", nv, None, -1)
                n += 1
                if in_history(nv, ov):
                    t.fail(f"error: nv {nv} in returned values\n{ov}\n")

        spawn_clients_and_wait(self, cfg, nclient, client_func)

        counts = [upto for _ in range(nclient)]
        from server import key_to_shard
        print(f"[DEBUG] Final key requested: 'k', shard {key_to_shard('k', cfg.nshards)}")
        vx = get(cfg, ck, "k", None, -1)
        check_concurrent_appends(self, vx, counts)

        cfg.end()

def check(t: unittest.TestCase, ck, key: str, value: str):
    v = ck.get(key)
    if v != value:
        t.fail(f"get({key}): expected:\n{value}\nreceived:\n{v}")

def randstring(n):
    b = os.urandom(2 * n)
    s = base64.urlsafe_b64encode(b).decode('utf-8')
    return s[:n]

# Test static 3-way sharding
class TestStaticShards(unittest.TestCase):
    def test_static_shards(self):
        cfg = make_shard_config(self, 3, 2, False)
        ck = cfg.make_client()

        n = 10
        ka = [str(i) for i in range(n)]
        va = [randstring(20) for i in range(n)]
        for i in range(n):
            ck.put(ka[i], va[i])
        for i in range(n):
            check(self, ck, ka[i], va[i])

        # make sure that the data really is sharded by
        # shutting down two shards and checking that some
        # get()s don't succeed.
        cfg.stop_server(1)
        cfg.stop_server(2)

        ch = queue.Queue()
        for xi in range(n):
            ck1 = cfg.make_client() # only one call allowed per client

            def client_func(i):
                try:
                    v = ck1.get(ka[i])
                    if v != va[i]:
                        ch.put(f"get({ka[i]}): expected:\n{va[i]}\nreceived:\n{v}")  # mismatch
                    else:
                        ch.put("OK")  # match
                except Exception:
                    pass  # no put = counts as failure

            threading.Thread(target=client_func, args=(xi,)).start()

        # wait a bit, only about 2/3 of the get()s should succeed.
        ndone = 0
        done = False
        while not done:
            try:
                err = ch.get(timeout=2)
                if err and not err.endswith("__FAIL__"):
                    logging.fatal(err)
                    ndone += 1
            except queue.Empty:
                done = True

        accept_range = (int(n*2/3)-1, int(n*2/3)+1)
        if ndone < accept_range[0] or ndone > accept_range[1]:
            self.fail(f"expected {accept_range[0]}-{accept_range[1]} completions with one shard dead; got {ndone}")

        # bring the crashed shard/group back to life
        cfg.start_server(1)
        cfg.start_server(2)
        for i in range(n):
            check(self, ck, ka[i], va[i])

        print("  ... Passed")

# do servers reject operations on shards for
# which they are not responsible?
class TestRejection(unittest.TestCase):
    def test_rejection(self):
        print("Test: rejection ...")

        cfg = make_shard_config(self, 3, 2, False)
        ck = cfg.make_client()

        n = 10
        ka = [str(i) for i in range(n)]
        va = [randstring(20) for i in range(n)]
        for i in range(n):
            ck.put(ka[i], va[i])
        for i in range(n):
            check(self, ck, ka[i], va[i])
        for sid in range(1, len(cfg.kvservers)):
            cfg.stop_server(sid)
        # now create a separate config that has only server
        # handling all the shards. The k/v server still uses
        # the original config, so the k/v servers still think
        # the shards are divided between the k/v servers.
        new_cfg = Config(self)
        new_cfg.net = cfg.net
        new_cfg.nservers = 1
        new_cfg.kvservers = cfg.kvservers[:1]
        new_cfg.running_servers = set([0])
        new_cfg.shard_to_servers = cfg.shard_to_servers

        # ask clients that use the new config to fetch keys.
        # they'll send all requests to a single k/v server.
        # 2/3 the requests should be rejected due to being sent to
        # the k/v server that doesn't think it is handling the shard.
        ch = queue.Queue()
        for xi in range(n):
            ck1 = new_cfg.make_client() # only one call allowed per client

            def client_func(i):  # i is from args=(xi,)
                try:
                    v = ck1.get(ka[i])
                    if v == "__FAIL__":
                        ch.put("__FAIL__")
                    elif v != va[i]:
                        ch.put(f"get({ka[i]}): expected:\n{va[i]}\nreceived:\n{v}")
                    else:
                        ch.put("OK")
                except Exception as e:
                    ch.put(f"Exception: {e}")

            threading.Thread(target=client_func, args=(xi,)).start()

        # wait a bit, only about 2/3 of the get()s should succeed.
        ndone = 0
        done = False
        while not done:
            try:
                err = ch.get(timeout=4)
                if err == "OK":
                    ndone += 1
                elif "__FAIL__" in err:
                    continue  # Ignore failures
                else:
                    logging.fatal(err)
            except queue.Empty:
                done = True

        accept_range = (int(n*2/3)-1, int(n*2/3)+1)
        if ndone < accept_range[0] or ndone > accept_range[1]:
            self.fail(f"expected {accept_range[0]}-{accept_range[1]} completions; got {ndone}")

        print("  ... Passed")

# Test: unreliable net, many clients
class TestUnreliableShards(unittest.TestCase):
    def test_unreliable_shards(self):
        generic_test(self, 5, (5, 3), True, False)
