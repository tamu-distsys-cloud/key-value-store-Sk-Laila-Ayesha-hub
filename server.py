# This file was developed with guidance and code suggestions from OpenAI's ChatGPT (https://chat.openai.com)
# and GitHub Copilot by Microsoft (https://github.com/features/copilot).
# These tools were used to understand distributed key-value store implementation concepts, debug test failures,
# and improve retry logic, replication, and linearizability handling.
# All final implementations were reviewed and tested manually to ensure correctness.




import threading
import time

class GetArgs:
    def __init__(self, key):
        self.key = key

class GetReply:
    def __init__(self, value=""):
        self.value = str(value) if value is not None else ""

class PutAppendArgs:
    def __init__(self, key=None, value=None, op=None, request_id=None):
        self.key = key
        self.value = value
        self.op = op
        self.request_id = request_id


class PutAppendReply:
    def __init__(self, value=""):
        print(f"[DEBUG] PutAppendReply returning: {repr(value)}")
        self.value = str(value) if value is not None else ""

def key_to_shard(key: str, nshards: int) -> int:
    try:
        return int(key) % nshards
    except ValueError:
        return sum(ord(c) for c in key) % nshards

class KVServer:
    def __init__(self, config, srvid, reliable=False):
        self.cfg = config
        self.my_id = srvid
        self.reliable = reliable

        self.nshards = config.nshards
        self.shard_id = self.find_my_shard()
        self.replica_ids = []
        self.replicas = []
        self.store = {}
        self.processed_requests = {}
        self.mu = threading.Lock()

        self.update_replica_ids()
        print(f"[ALIVE] This KVServer instance is from file: {__file__}")
        print(f"[DEBUG] KVServer {self.my_id} method map: {[m for m in dir(self) if not m.startswith('_')]}")

    def find_my_shard(self):
        for sid, rids in self.cfg.shard_to_servers.items():
            if self.my_id in rids:
                print(f"[DEBUG] Server {self.my_id} assigned to shard {sid}")
                return sid
        print(f"[DEBUG] Server {self.my_id} not assigned to any shard")
        return None

    def update_replica_ids(self):
        self.replica_ids = self.cfg.shard_to_servers.get(self.shard_id, [self.my_id])
        self.replicas = [self.cfg.make_client_end(rid) for rid in self.replica_ids]
        print(f"[DEBUG INIT] KVServer {self.my_id} replica_ids={self.replica_ids}")

    def is_primary(self):
        return self.replica_ids and self.my_id == self.replica_ids[0]

    def owns_shard(self, key: str) -> bool:
        shard_id = key_to_shard(key, self.nshards)
        owns = (
                self.cfg.net.is_server_enabled(self.my_id) and
                (self.shard_id == shard_id) and
                (self.my_id in self.replica_ids)
        )
        print(f"[DEBUG] Server {self.my_id}: shard_id={self.shard_id}, key '{key}' → shard {shard_id} → {'✓' if owns else '✗'}")
        return owns

    def Get(self, args: GetArgs) -> GetReply:
        print(f"[TRACE] KVServer {self.my_id} received Get on key={args.key}")
        reply = GetReply()
        if not self.owns_shard(args.key):
            print(f"[DEBUG] Server {self.my_id} rejected Get('{args.key}') — doesn't own shard")
            reply.value = "__FAIL__"
            return reply

        with self.mu:
            reply.value = str(self.store.get(args.key, ""))
            print(f"[DEBUG] KVServer {self.my_id} Get('{args.key}') returning: '{reply.value}'")
        return reply

    def Put(self, args: PutAppendArgs) -> PutAppendReply:
        print(f"[TRACE] KVServer {self.my_id} received {args.request_id} on key={args.key} [Put]")
        return self._handle_request(args)

    def Append(self, args: PutAppendArgs) -> PutAppendReply:
        print(f"[TRACE] KVServer {self.my_id} received {args.request_id} on key={args.key} [Append]")
        return self._handle_request(args)

    def _handle_request(self, args: PutAppendArgs) -> PutAppendReply:
        if not self.owns_shard(args.key):
            return PutAppendReply("__FAIL__")

        # Early dedup check before network replication — avoids redundant work
        with self.mu:
            if args.request_id in self.processed_requests:
                return self.processed_requests[args.request_id]

        # STEP 1: Replicate before applying state
        if args.op in ("Put", "Append"):
            success = self.forward_to_replicas(args)
            if success == 0:
                return PutAppendReply("__FAIL__")

        # STEP 2: Only after replication, we re-check deduplication and apply mutation
        with self.mu:
            if args.request_id in self.processed_requests:
                return self.processed_requests[args.request_id]

            old_value = self.store.get(args.key, "")
            if args.op == "Put":
                self.store[args.key] = args.value
            elif args.op == "Append":
                self.store[args.key] = old_value + args.value

            reply = PutAppendReply(old_value)
            self.processed_requests[args.request_id] = reply
            return reply

    def Replicate(self, args: PutAppendArgs) -> PutAppendReply:
        if not self.owns_shard(args.key):
            return PutAppendReply("__FAIL__")

        with self.mu:
            if args.request_id in self.processed_requests:
                return self.processed_requests[args.request_id]

            old_value = self.store.get(args.key, "")
            if args.op == "Put":
                self.store[args.key] = args.value
            elif args.op == "Append":
                self.store[args.key] = old_value + args.value

            reply = PutAppendReply(old_value)
            self.processed_requests[args.request_id] = reply
            return reply

    def forward_to_replicas(self, args):
        success = 0
        for rep_id in self.replica_ids:
            if rep_id == self.my_id:
                continue
            client_end = next((ce for i, ce in enumerate(self.replicas) if self.replica_ids[i] == rep_id), None)
            if not client_end:
                continue

            for attempt in range(5):
                try:
                    if self.cfg.net.is_server_enabled(rep_id):
                        reply = client_end.call("KVServer.Replicate", args)
                        print(f"[DEBUG] Replication to server {rep_id} succeeded on attempt {attempt}")
                        if reply and reply.value != "__FAIL__":
                            success += 1
                            break
                except Exception:
                    pass
                if not self.reliable:
                    time.sleep(min(0.05 * (2 ** attempt), 1.0))
            else:
                print(f"[DEBUG] Replication to server {rep_id} failed after all retries")
        return success

    def __str__(self):
        return f"KVServer({self.my_id})"
