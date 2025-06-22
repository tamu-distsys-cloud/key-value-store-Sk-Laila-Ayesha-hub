# client.py
import time
import threading
import uuid
from typing import Dict
from labrpc.labrpc import ClientEnd
from server import GetArgs, PutAppendArgs, PutAppendReply, GetReply

class Clerk:
    def __init__(self, servers, config, nshards, shard_to_servers):
        self.servers: Dict[int, ClientEnd] = servers
        print(f"[DEBUG] Clerk connected to servers: {list(self.servers.keys())}")
        self.cfg = config
        self.nshards = nshards
        self.shard_to_servers = shard_to_servers
        self.retry_limit = 50
        self._req_id = 0
        self.mu = threading.Lock()

    def key_to_shard(self, key: str) -> int:
        try:
            return int(key) % self.nshards
        except ValueError:
            return sum(ord(c) for c in key) % self.nshards

    def next_request_id(self):
        with self.mu:
            self._req_id += 1
            return f"{uuid.uuid4()}-{self._req_id}"

    def get(self, key: str) -> str:
        shard_id = self.key_to_shard(key)
        servers = self.shard_to_servers.get(shard_id, [])
        if not servers:
            return "__FAIL__"

        args = GetArgs(key)

        for attempt in range(self.retry_limit):
            for sid in servers:
                try:
                    if self.cfg.net.is_server_enabled(sid):
                        print(f"[DEBUG] Clerk get retry {attempt}, contacting server {sid}")
                        reply: GetReply = self.servers[sid].call("KVServer.Get", args)
                        if reply and reply.value != "__FAIL__":
                            return reply.value
                except Exception:
                    pass
            time.sleep(min(0.05 * (2 ** attempt), 1.0))

        return "__FAIL__"

    def put_append(self, key: str, value: str, op: str) -> str:
        shard_id = self.key_to_shard(key)
        servers = self.shard_to_servers.get(shard_id, [])
        if not servers:
            return "__FAIL__"

        request_id = self.next_request_id()
        args = PutAppendArgs(key, value, op, request_id)

        for attempt in range(self.retry_limit):
            for sid in servers:
                try:
                    if self.cfg.net.is_server_enabled(sid):
                        reply: PutAppendReply = self.servers[sid].call(f"KVServer.{op}", args)
                        # Accept any reply that looks like a successful ack
                        if reply and reply.value != "__FAIL__":
                            return reply.value
                except Exception:
                    pass

            # Optional: log progress to track flaky starts
            print(f"[RETRY] {op}({key}, {value}) request {request_id} → retry {attempt}")
            time.sleep(min(0.05 * (2 ** attempt), 1.0))

        print(
            f"[DEBUG] Clerk failed {op}({key}, {value}) after {self.retry_limit} retries → giving up. request_id={request_id}")
        return "__FAIL__"

    def put(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Put")

    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")