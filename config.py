# This file was edited with guidance and code suggestions from OpenAI's ChatGPT (https://chat.openai.com)
# and GitHub Copilot by Microsoft (https://github.com/features/copilot).

import os
import random
import math
import time
import threading
import unittest
import logging
import base64

from labrpc.labrpc import Network, Service, Server
from client import Clerk
from server import KVServer, key_to_shard

def randstring(n):
    b = os.urandom(2 * n)
    s = base64.urlsafe_b64encode(b).decode('utf-8')
    return s[:n]

def make_seed():
    max_val = 1 << 62
    bigx = random.randint(0, max_val)
    return bigx

class Config:
    def __init__(self, t: unittest.TestCase):
        self.end_clients = {}
        self.mu = threading.Lock()
        self.t = t
        self.endnames = []
        self.srvid_to_endname = {}  # ← initialize early to avoid missing attribute
        self.net = Network()
        self.nservers = 0
        self.kvservers = None
        self.running_servers = set()
        self.clerks = {}
        self.start = time.time()
        self.t0 = None
        self.rpcs0 = 0
        self.ops = 0
        self.nreplicas = 1
        self.nshards=3

    def is_connected(self, server_id):
        return self.net.is_connected(server_id)

    def cleanup(self):
        with self.mu:
            self.net.cleanup()

    def get_shard_replicas(self, shard_id):
        return self.shard_to_servers[shard_id]

    def make_client(self):
        with self.mu:
            endnames = [randstring(20) for i in range(self.nservers)]
            ends = [self.net.make_end(endname) for endname in endnames]
            for srvid in range(self.nservers):
                self.net.connect(endnames[srvid], srvid)
            shard_to_servers = {}
            for idx, kv in enumerate(self.kvservers):
                shard_id = kv.shard_id
                if shard_id not in shard_to_servers:
                    shard_to_servers[shard_id] = []
                shard_to_servers[shard_id].append(idx)

            self.shard_to_servers = shard_to_servers  # ✅ FIXED: now Clerk can access it via self.cfg.shard_to_servers

            ck = Clerk({i: self.make_client_end(i) for i in range(self.nservers)}, self, self.nshards, shard_to_servers)

            self.clerks[ck] = endnames
            self.connect_client_unlocked(ck)


        return ck

    def make_client_end(self, server_id):
        endname = self.srvid_to_endname.get(server_id, str(server_id))
        if endname in self.end_clients:
            return self.end_clients[endname]

        end = self.net.make_end(endname)
        self.net.connect(endname, server_id)
        self.net.enable(endname, server_id in self.running_servers)

        self.end_clients[endname] = end
        self.endnames.append(endname)
        return end

    def delete_client(self, ck):
        with self.mu:
            for v in self.clerks[ck]:
                self.net.delete_end(v)
            del self.clerks[ck]

    def connect_client_unlocked(self, ck):
        endnames = self.clerks[ck]
        for srvid in range(self.nservers):
            self.net.enable(endnames[srvid], srvid in self.running_servers)

    def connect_client(self, ck):
        with self.mu:
            self.connect_client_unlocked(ck)

    def start_cluster(self, nservers):
        self.nservers = nservers
        self.kvservers = [None] * nservers
        for srvid in range(nservers):
            self.kvservers[srvid] = KVServer(self, srvid=srvid,
                                             reliable=True)  # or use reliable=False if simulating unreliability
            kvsvc = Service(self.kvservers[srvid])
            srv = Server()
            srv.add_service(kvsvc)
            self.net.add_server(srvid, srv)
            self.running_servers.add(srvid)

    def stop_server(self, srvid):
        print(f"[DEBUG] stop_server({srvid}) → endnames = {self.endnames}")  # ← add this line
        with self.mu:
            if srvid not in self.running_servers:
                return
            for ck in self.clerks.keys():
                endnames = self.clerks[ck]
                if srvid < len(endnames):
                    self.net.enable(endnames[srvid], False)

            self.running_servers.remove(srvid)

    def start_server(self, srvid):
        with self.mu:
            if srvid in self.running_servers:
                return
            for ck in self.clerks.keys():
                endnames = self.clerks[ck]
                assert srvid < len(endnames)
                self.net.enable(endnames[srvid], True)
            self.running_servers.add(srvid)

    def begin(self, description):
        print(f"{description} ...\n")
        self.t0 = time.time()
        self.rpcs0 = self.rpc_total()
        with self.mu:
            self.ops = 0

    def op(self):
        with self.mu:
            self.ops += 1

    def rpc_total(self):
        return self.net.get_total_count()

    def end(self):
        if self.t.defaultTestResult().wasSuccessful():
            t = time.time() - self.t0
            nrpc = self.rpc_total() - self.rpcs0
            with self.mu:
                ops = self.ops
            print("  ... Passed --")
            print(f" t {t} nrpc {nrpc} ops {ops}\n")

def make_single_config(test, unreliable):
    from config import make_shard_config
    return make_shard_config(test, nshards=1, nreplicas=3, unreliable=unreliable)

# Updated make_shard_config with debug tracing and Clerk registration

def make_shard_config(t, nshards, nreplicas, unreliable):
    from server import KVServer
    from client import Clerk

    cfg = Config(t)
    cfg.nshards = nshards
    print(f"[DEBUG] Key '0' maps to shard {key_to_shard('0', cfg.nshards)}")
    cfg.nreplicas = nreplicas
    cfg.clerks = {}
    cfg.start = time.time()

    total_servers = nshards * nreplicas
    cfg.nservers = total_servers
    cfg.kvservers = [None] * total_servers

    ends = [cfg.net.make_end(i) for i in range(total_servers)]
    cfg.srvid_to_endname = {}

    shard_to_servers = {}
    cfg.shard_to_servers = shard_to_servers
    server_index = 0

    servers = []

    for shard_id in range(nshards):
        shard_to_servers[shard_id] = []
        for _ in range(nreplicas):
            kv = KVServer(cfg, srvid=server_index, reliable=True)
            kv.me = ends[server_index]
            cfg.end_clients[str(server_index)] = kv.me  # 🧠 add this line to store the mapping
            endname = kv.me.endname
            cfg.srvid_to_endname[server_index] = endname

            cfg.net.connect(endname, server_index)

            servers.append(kv)

            cfg.kvservers[server_index] = kv


            srv = Server()
            srv.add_service(Service(kv))
            cfg.net.add_server(server_index, srv)

            print(f"[CHECK] Assigning server {server_index} to shard {shard_id}")  # ← Add this here

            shard_to_servers[shard_id].append(server_index)
            server_index += 1
    print(f"[CHECK] Final shard-to-servers map: {shard_to_servers}")
    for srvid in range(cfg.nservers):
        cfg.net.connect(str(srvid), srvid)

    # Assign shard IDs after the full map is built
    for kv in servers:
        kv.shard_id = kv.find_my_shard()
        print(f"[DEBUG] Server {kv.my_id} assigned to shard {kv.shard_id}")

    # Register with network now that shard IDs are set
    for kv in servers:
        cfg.running_servers.add(kv.my_id)
        cfg.net.connect(kv.me.endname, kv.my_id)
        cfg.net.enable(kv.me.endname, True)

    for shard_id, server_indices in shard_to_servers.items():
        replicas = [cfg.kvservers[i].me for i in server_indices]
        for i in server_indices:
            cfg.kvservers[i].replica_ids = server_indices[:]
            cfg.kvservers[i].replicas = replicas

    for kv in cfg.kvservers:
        kv.update_replica_ids()
        kv.replicas = [cfg.make_client_end(rep_id) for rep_id in kv.replica_ids]

    cfg.net.reliable(not unreliable)

    def make_client():
        ck = Clerk(
            servers={i: cfg.make_client_end(i) for i in range(total_servers)},
            config=cfg,
            nshards=nshards,
            shard_to_servers=shard_to_servers
        )
        cfg.clerks[ck] = []
        return ck

    cfg.make_client = make_client

    print("[DEBUG] Shard to Servers Map:")
    for shard, server_ids in cfg.shard_to_servers.items():
        print(f"  Shard {shard}: {server_ids}")

    print(f"[DEBUG] Total servers: {total_servers}")
    print(f"[DEBUG] Config registered {len(cfg.kvservers)} KVServer instances")

    return cfg
