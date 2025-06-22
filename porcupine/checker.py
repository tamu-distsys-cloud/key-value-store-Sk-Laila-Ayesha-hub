import threading
import time
from typing import List, Tuple, Any, Dict

from porcupine.model import *
from porcupine.bitset import BitSet

class Entry:
    def __init__(self, is_return: bool, value: Any, id: int, time: int, client_id: int):
        self.is_return = is_return
        self.value = value
        self.id = id
        self.time = time
        self.client_id = client_id

class LinearizationInfo:
    def __init__(self, history: List[List[Entry]], partial_linearizations: List[List[List[int]]]):
        self.history = history
        self.partial_linearizations = partial_linearizations

class ByTime:
    def __init__(self, entries: List[Entry]):
        self.entries = entries

    def __len__(self):
        return len(self.entries)

    def __getitem__(self, idx):
        return self.entries[idx]

    def __setitem__(self, idx, value):
        self.entries[idx] = value

    def sort(self):
        self.entries.sort(key=lambda e: (e.time, not e.is_return and 0 or 1))

def make_entries(history: List[Operation]) -> List[Entry]:
    entries = []
    id_counter = 0
    for elem in history:
        entries.append(Entry(False, elem.input, id_counter, elem.call_time, elem.client_id))
        entries.append(Entry(True, elem.output, id_counter, elem.response_time, elem.client_id))
        id_counter += 1
    entries_by_time = ByTime(entries)
    entries_by_time.sort()
    return entries

class Node:
    def __init__(self, value, match, id, next_node = None, prev_node = None):
        self.value = value
        self.match = match  # Call if match is None, otherwise Return
        self.id = id
        self.next = next_node
        self.prev = prev_node

def insert_before(n: Node, mark: Node) -> Node:
    if mark:
        before_mark = mark.prev
        mark.prev = n
        n.next = mark
        if before_mark:
            n.prev = before_mark
            before_mark.next = n
    return n

def length(n: Node) -> int:
    l = 0
    while n:
        n = n.next
        l += 1
    return l

def renumber(events: List[Event]) -> List[Event]:
    e = []
    m = {}  # renumbering
    id_counter = 0
    for v in events:
        if v.event_id in m:
            e.append(Event(v.client_id, v.is_return, v.value, m[v.Id]))
        else:
            e.append(Event(v.client_id, v.is_return, v.value, id_counter))
            m[v.event_id] = id_counter
            id_counter += 1
    return e

def convert_entries(events: List[Event]) -> List[Entry]:
    entries = []
    for i, elem in enumerate(events):
        is_return = False
        if elem.is_return:
            is_return = True
        # Use index as "time"
        entries.append(Entry(is_return, elem.value, elem.id, i, elem.client_id))
    return entries

def make_linked_entries(entries: List[Entry]) -> Node:
    root = None
    match = {}
    for i in range(len(entries) - 1, -1, -1):
        elem = entries[i]
        if elem.is_return:
            entry_node = Node(elem.value, None, elem.id)
            match[elem.id] = entry_node
            insert_before(entry_node, root)
            root = entry_node
        else:
            entry_node = Node(elem.value, match[elem.id], elem.id)
            insert_before(entry_node, root)
            root = entry_node
    return root

class CacheEntry:
    def __init__(self, linearized, state):
        self.linearized = linearized
        self.state = state

def cache_contains(model: Model, cache: Dict[int, List[CacheEntry]], entry: CacheEntry) -> bool:
    for elem in cache.get(entry.linearized.hash(), []):
        if entry.linearized.equals(elem.linearized) and model.Equal(entry.state, elem.state):
            return True
    return False

class CallsEntry:
    def __init__(self, entry: Node, state: Any):
        self.entry = entry
        self.state = state

def lift(entry: Node):
    entry.prev.next = entry.next
    entry.next.prev = entry.prev
    match = entry.match
    match.prev.next = match.next
    if match.next:
        match.next.prev = match.prev

def unlift(entry: Node):
    match = entry.match
    match.prev.next = match
    if match.next:
        match.next.prev = match
    entry.prev.next = entry
    entry.next.prev = entry

def check_single(model: Model, history: List[Entry], compute_partial: bool, kill: threading.Event) -> Tuple[bool, List[List[int]]]:
    entry = make_linked_entries(history)
    n = length(entry) // 2
    linearized = BitSet(n)
    cache = {}  # map from hash to cache entry
    calls = []
    longest = [None] * n  # longest linearizable prefix that includes the given entry

    state = model.init()
    head_entry = insert_before(Node(None, None, -1), entry)
    while head_entry.next:
        if kill.is_set():
            return False, longest
        if entry.match:
            matching = entry.match  # the return entry
            ok, new_state = model.step(state, entry.value, matching.value)
            if ok:
                new_linearized = linearized.clone().set(entry.id)
                new_cache_entry = CacheEntry(new_linearized, new_state)
                if not cache_contains(model, cache, new_cache_entry):
                    hash_value = new_linearized.hash()
                    if hash_value not in cache:
                        cache[hash_value] = []
                    cache[hash_value].append(new_cache_entry)
                    calls.append(CallsEntry(entry, state))
                    state = new_state
                    linearized.set(entry.id)
                    lift(entry)
                    entry = head_entry.next
                else:
                    entry = entry.next
            else:
                entry = entry.next
        else:
            if not calls:
                return False, longest
            # longest
            if compute_partial:
                calls_len = len(calls)
                seq = None
                for v in calls:
                    if longest[v.entry.id] is None or calls_len > len(longest[v.entry.id]):
                        # create seq lazily
                        if seq is None:
                            seq = [v.entry.id for v in calls]
                        longest[v.entry.id] = seq
            calls_top = calls.pop()
            entry = calls_top.entry
            state = calls_top.state
            linearized.clear(entry.id)
            unlift(entry)
            entry = entry.next
    # longest linearization is the complete linearization, which is calls
    seq = [v.entry.id for v in calls]
    for i in range(n):
        longest[i] = seq
    return True, longest

def fill_default(model: Model) -> Model:
    if model.partition is None:
        model.partition = no_partition
    if model.partition_event is None:
        model.partition_event = no_partition_event
    if model.equal is None:
        model.equal = shallow_equal
    if model.describe_operation is None:
        model.describe_operation = default_describe_operation
    if model.describe_state is None:
        model.describe_state = default_describe_state
    return model

def check_parallel(model: Model, history: List[List[Entry]], compute_info: bool, timeout: float) -> Tuple[str, LinearizationInfo]:
    ok = True
    timed_out = False
    results = []
    longest = [None] * len(history)
    kill = threading.Event()

    def worker(i: int, subhistory: List[Entry]):
        nonlocal ok
        single_ok, l = check_single(model, subhistory, compute_info, kill)
        longest[i] = l
        results.append(single_ok)
        if not single_ok and not compute_info:
            kill.set()

    threads = [threading.Thread(target=worker, args=(i, subhistory)) for i, subhistory in enumerate(history)]
    for t in threads:
        t.start()

    if timeout > 0:
        timeout_event = threading.Event()
        timeout_thread = threading.Thread(target=lambda: (time.sleep(timeout), timeout_event.set()))
        timeout_thread.start()

    for t in threads:
        t.join()

    if timeout_event.is_set():
        timed_out = True

    if compute_info:
        # make sure we've waited for all threads to finish,
        # otherwise we might race on access to longest[]
        for t in threads:
            t.join()

        # return longest linearizable prefixes that include each history element
        partial_linearizations = []
        for sub_longest in longest:
            partials = []
            seen = set()
            for v in sub_longest:
                if v is not None and tuple(v) not in seen:
                    seen.add(tuple(v))
                    partials.append(v)

            partial_linearizations.append(partials)

        info = LinearizationInfo(history, partial_linearizations)
    else:
        info = None

    if not ok:
        result = "Illegal"
    elif timed_out:
        result = "Unknown"
    else:
        result = "Ok"

    return result, info

def check_events(model: Model, history: List[Event], verbose: bool, timeout: float) -> Tuple[str, LinearizationInfo]:
    model = fill_default(model)
    partitions = model.partition_event(history)
    l = []
    for i in range(len(partitions)):
        l.append(convert_entries(renumber(partitions[i])))
    return check_parallel(model, l, verbose, timeout)

def check_operations(model: Model, history: List[Operation], verbose: bool, timeout: float) -> Tuple[str, LinearizationInfo]:
    model = fill_default(model)
    partitions = model.partition(history)
    l = []
    for i in range(len(partitions)):
        l.append(convert_entries(make_entries(partitions[i])))
    return check_parallel(model, l, verbose, timeout)

