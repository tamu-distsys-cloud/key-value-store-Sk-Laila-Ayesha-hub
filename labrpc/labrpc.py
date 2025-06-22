import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import threading
import logging
import random
import time
import io
import queue
from collections import defaultdict

from labgob.labgob import LabEncoder, LabDecoder  # ✅ Only once, after sys.path is set

logging.basicConfig(level=logging.FATAL)

class ReqMsg:
    def __init__(self, endname, svcMeth, argsType, args):
        self.endname = endname  # name of sending ClientEnd
        self.svcMeth = svcMeth  # e.g. "Raft.AppendEntries"
        self.argsType = argsType
        self.args = args
        self.replyCh = queue.Queue()

class ReplyMsg:
    def __init__(self, ok, reply):
        self.ok = ok
        self.reply = reply

class ClientEnd:
    def __init__(self, endname, network):
        self.endname = endname  # this end-point's name
        self.ch = network.endCh
        self.done = network.done

    def call(self, svcMeth, args):
        qb = io.BytesIO()
        LabEncoder(qb).encode(args);
        req = ReqMsg(self.endname, svcMeth, type(args), qb.getvalue())

        # Send the request
        try:
            self.ch.put(req, block=False)
        except queue.Full:
            raise TimeoutError()

        # Wait for the reply
        rep = req.replyCh.get()
        if rep.ok:
            return LabDecoder(io.BytesIO(rep.reply)).decode()
        else:
            raise TimeoutError()

class Network:
    def __init__(self):
        self.mu = threading.Lock()
        self.isreliable = True
        self.longDelays = False
        self.longReordering = False
        self.ends = {}
        self.enabled = {}
        self.servers = {}
        self.connections = {}
        self.endCh = queue.Queue()
        self.done = threading.Event()
        self.count = 0
        self.bytes = 0

        # single thread to handle all ClientEnd.call()s
        threading.Thread(target=self._process_requests, daemon=True).start()

    def is_server_enabled(self, server_id):
        """Returns True if any end is enabled to this server"""
        with self.mu:
            for endname, enabled in self.enabled.items():
                if self.connections.get(endname) == server_id and enabled:
                    return True
            return False

    def cleanup(self):
        self.done.set()

    def reliable(self, yes):
        with self.mu:
            self.isreliable = yes

    def long_reordering(self, yes):
        with self.mu:
            self.longReordering = yes

    def long_delays(self, yes):
        with self.mu:
            self.longDelays = yes

    def _process_requests(self):
        while not self.done.is_set():
            try:
                xreq = self.endCh.get(timeout=0.1)
            except queue.Empty:
                continue

            with self.mu:
                self.count += 1
                self.bytes += len(xreq.args)

            threading.Thread(target=self.process_req, args=(xreq,), daemon=True).start()

    def read_endname_info(self, endname):
        with self.mu:
            enabled = self.enabled[endname]
            servername = self.connections[endname]
            server = self.servers.get(servername)
            isreliable = self.isreliable
            long_reordering = self.longReordering

            print(f"[DEBUG] read_endname_info(): endname={endname}, servername={servername}, server={type(server).__name__ if server else None}")
        return enabled, servername, server, isreliable, long_reordering

    def is_server_dead(self, endname, servername, server):
        with self.mu:
            return not self.enabled[endname] or self.servers[servername] != server

    def process_req(self, req):
        enabled, servername, server, isreliable, long_reordering = self.read_endname_info(req.endname)
        if enabled and (servername is not None) and (server is not None):
            if not isreliable:
                time.sleep(random.randint(0, 27) / 1000)

            if not isreliable and random.randint(0, 999) < 100:
                req.replyCh.put(ReplyMsg(False, None))
                return

            ech = queue.Queue()

            def dispatch():
                print(f"[DEBUG] Network: dispatching {req.svcMeth} to server {servername}")
                r = server.dispatch(req)
                ech.put(r)

            threading.Thread(target=dispatch, daemon=True).start()

            reply = None
            reply_ok = False
            server_dead = False

            while not reply_ok and not server_dead:
                try:
                    reply = ech.get(timeout=0.1)
                    reply_ok = True
                except queue.Empty:
                    server_dead = self.is_server_dead(req.endname, servername, server)

            if not reply_ok or server_dead:
                req.replyCh.put(ReplyMsg(False, None))
            elif not isreliable and random.randint(0, 999) < 100:
                req.replyCh.put(ReplyMsg(False, None))
            elif long_reordering and random.randint(0, 899) < 600:
                ms = 200 + random.randint(0, 2000)
                threading.Timer(ms / 1000, lambda: req.replyCh.put(reply)).start()
            else:
                req.replyCh.put(reply)
        else:
            ms = random.randint(0, 7000) if self.longDelays else random.randint(0, 100)
            threading.Timer(ms / 1000, lambda: req.replyCh.put(ReplyMsg(False, None))).start()

    def make_end(self, endname):
        with self.mu:
            if endname in self.ends:
                logging.fatal(f"MakeEnd: {endname} already exists")

            e = ClientEnd(endname, self)
            self.ends[endname] = e
            self.enabled[endname] = False
            self.connections[endname] = None

        return e

    def delete_end(self, endname):
        with self.mu:
            if endname not in self.ends:
                logging.fatal(f"MakeEnd: {endname} doesn't exist")
            del self.ends[endname]
            del self.enabled[endname]
            del self.connections[endname]

    def add_server(self, servername, server):
        with self.mu:
            self.servers[servername] = server
            print(f"[DEBUG] add_server: servername={servername} (type={type(servername)}), all server keys={list(self.servers.keys())}")

    def delete_server(self, servername):
        with self.mu:
            self.servers[servername] = None

    def connect(self, endname, servername):
        with self.mu:
            self.connections[endname] = servername

    def enable(self, endname, enabled):
        with self.mu:
            self.enabled[endname] = enabled

    def get_count(self, servername):
        with self.mu:
            server = self.servers[servername]
        return server.get_count() if server else 0

    def get_total_count(self):
        return self.count

    def get_total_bytes(self):
        return self.bytes

class Server:
    def __init__(self):
        self.mu = threading.Lock()
        self.services = {}
        self.count = 0

    def add_service(self, svc):
        with self.mu:
            self.services[svc.name] = svc

    def dispatch(self, req):

        with self.mu:
            self.count += 1

            dot = req.svcMeth.rindex('.')
            service_name = req.svcMeth[:dot]
            method_name = req.svcMeth[dot + 1:]
            service = self.services.get(service_name)
            print(f"[DEBUG] dispatch(): svcMeth = {req.svcMeth}, split → service={service_name}, method={method_name}")

        if service:
            print(f"[DEBUG] dispatch(): Found service {service_name}, calling method {method_name}")
            return service.dispatch(method_name, req)
        else:
            choices = list(self.services.keys())
            logging.fatal(f"labrpc.Server.dispatch(): unknown service {service_name} in {req.svcMeth}; expecting one of {choices}")
            return ReplyMsg(False, None)

    def get_count(self):
        with self.mu:
            return self.count



class Service:
    def __init__(self, rcvr):
        self.name = type(rcvr).__name__
        self.rcvr = rcvr
        self.methods = {}
        print(f"[HOOK] Registering Service for rcvr={rcvr}, type={type(rcvr)}")
        for method_name in dir(rcvr):
            if method_name.startswith('_'):
                continue
            method = getattr(rcvr, method_name)
            if callable(method):
                print(f"[HOOK] Adding method: {method_name} → {method} (type: {type(method)})")
                self.methods[method_name] = method
        print(f"[DEBUG] Registered service: {self.name}, methods: {list(self.methods.keys())}")

    def dispatch(self, methname, req):
        method = self.methods.get(methname)
        if method:
            # 🛠 Safely decode the argument
            try:
                args = LabDecoder(io.BytesIO(req.args)).decode()
                print(f"[TRACE] Dispatch decoded for method '{methname}': {vars(args)}")

            except Exception as e:
                print(f"[FATAL] Failed to decode args for method='{methname}': {e}")
                return ReplyMsg(False, None)

            # ✅ Call the method
            replyv = method(args)

            # 🔁 Encode the reply
            rb = io.BytesIO()
            LabEncoder(rb).encode(replyv)
            reply = rb.getvalue()
            return ReplyMsg(True, reply)
        else:
            choices = list(self.methods.keys())
            logging.fatal(f"labrpc.Service.dispatch(): unknown method {methname} in {req.svcMeth}; expecting one of {choices}")
            return ReplyMsg(False, None)
# ... all class definitions above (e.g., Service, Server, etc.)

if __name__ == "__main__":
    from server import PutAppendArgs  # or adjust the import path
    import io
    from labgob.labgob import LabEncoder, LabDecoder
    args = PutAppendArgs("k", "v", "Append", "req-123")
    buf = io.BytesIO()
    LabEncoder(buf).encode(args)

    decoded = LabDecoder(io.BytesIO(buf.getvalue())).decode()
    print(vars(decoded))
