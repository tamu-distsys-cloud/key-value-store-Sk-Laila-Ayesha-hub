import logging
import threading
from typing import Tuple, Any

debugging = False
#test commit
# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)

# Put or Append
class PutAppendArgs:
    # Add definitions here if needed
    def __init__(self, key, value):
        self.key = key
        self.value = value

class PutAppendReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key):
        self.key = key

class GetReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg

        # Your definitions here.

    def Get(self, args: GetArgs):
        reply = GetReply(None)

        # Your code here.

        return reply

    def Put(self, args: PutAppendArgs):
        reply = PutAppendReply(None)

        # Your code here.

        return reply

    def Append(self, args: PutAppendArgs):
        reply = PutAppendReply(None)

        # Your code here.

        return reply
