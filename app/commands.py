import sys
import asyncio
from time import time
from typing import NamedTuple

STORE = {}
WAITERS = {}


class StoreEntry(NamedTuple): 
    value: str | list[str] | list[tuple[str, dict[str, str]]]
    expiry_time: float | None
    redis_type: str


def run_ping(args: list[str]) -> bytes:
    return b'+PONG\r\n'


def run_echo(args: list[str]) -> bytes:
    return f'${len(args[0])}\r\n{args[0]}\r\n'.encode()


def run_type(args: list[str]) -> bytes:
    key = args[0]
    store_entry = STORE.get(key)

    # If the key doesn't exist
    if store_entry is None:
        return b'+none\r\n'
    
    return f'+{store_entry.redis_type}\r\n'.encode()


def run_set(args: list[str]) -> bytes:
    key = args[0]
    expiry_time = None

    if 'PX' in args: 
        expiry_time = time() + (int(args[3]) / 1000)
    elif 'EX' in args: 
        expiry_time = time() + int(args[3])

    STORE[key] = StoreEntry(value=args[1], expiry_time=expiry_time, redis_type='string')

    return b'+OK\r\n'


def run_get(args: list[str]) -> bytes:
    key = args[0]
    store_entry = STORE.get(key)

    if store_entry is None: 
        return b'$-1\r\n'
    elif store_entry.expiry_time is not None and time() > store_entry.expiry_time:  # If there's an expiry time and the current time exceeds it, the key is expired. 
        return b'$-1\r\n'
    
    return f'${len(store_entry.value)}\r\n{store_entry.value}\r\n'.encode()


def run_rpush(args: list[str]) -> bytes: 
    key, elements = args[0], args[1:]
    store_entry = STORE.get(key)
    lst = store_entry.value if store_entry is not None else []

    lst.extend(elements)
    STORE[key] = StoreEntry(value=lst, expiry_time=None, redis_type='list')

    event_list = WAITERS.get(key, [])
    if event_list: 
        event_list[0].set()

    return f':{len(lst)}\r\n'.encode()


def run_lpush(args: list[str]) -> bytes: 
    key, elements = args[0], args[1:]
    store_entry = STORE.get(key)
    lst = store_entry.value if store_entry is not None else []

    # Reverse elements, prepend them to the existing list, and store the result. 
    lst = elements[::-1] + lst  
    STORE[key] = StoreEntry(value=lst, expiry_time=None, redis_type='list')

    return f':{len(lst)}\r\n'.encode()


def run_lpop(args: list[str]) -> bytes: 
    key = args[0]
    store_entry = STORE.get(key)
    lst = store_entry.value if store_entry is not None else []
    
    if not lst:  
        return b'$-1\r\n'

    if len(args) == 1:  # No size argument provided; pop the first index only.
        elmt = lst.pop(0)
        STORE[key] = StoreEntry(value=lst, expiry_time=None, redis_type='list')
        return f'${len(elmt)}\r\n{elmt}\r\n'.encode()

    num = int(args[1])
    num_elmts_to_pop = min(num, len(lst))  # Refactored version (below)

    # if num > len(lst): 
    #     num_elmts_to_pop = len(lst) 
    # else: 
    #     num_elmts_to_pop = num
    
    # Refactored version (below)
    popped_elmts = lst[:num_elmts_to_pop]
    lst = lst[num_elmts_to_pop:]

    # popped_elmts = []
    # for elmt in lst[0:num_elmts_to_pop]: 
    #     popped_elmts.append(lst.pop(0))

    STORE[key] = StoreEntry(value=lst, expiry_time=None, redis_type='list')

    resp_array = [f'*{len(popped_elmts)}\r\n']
    for elmt in popped_elmts:
        resp_array.append(f'${len(elmt)}\r\n')
        resp_array.append(f'{elmt}\r\n')

    return ''.join(resp_array).encode()


async def run_blpop(args: list[str]) -> bytes:
    key = args[0]
    store_entry = STORE.get(key)
    lst = store_entry.value if store_entry is not None else []

    if not lst:
        event = asyncio.Event()
        event_list = WAITERS.get(key, [])
        event_list.append(event)
        WAITERS[key] = event_list
        
        timeout = float(args[1])
        timeout = None if timeout == 0 else timeout

        # Block until an element is pushed or the timeout expires.
        try:  
            await asyncio.wait_for(event.wait(), timeout)  
            lst = STORE[key].value  # Re-fetch the updated list after being unblocked. 
        except asyncio.TimeoutError:
            return b'*-1\r\n'
    else:
        event_list = WAITERS.get(key, [])
        if event_list:
            event_list.pop(0)  # Remove the handled event. 
            WAITERS[key] = event_list  # Update the events list and store it. 
            
    elmt = lst.pop(0)
    STORE[key] = StoreEntry(value=lst, expiry_time=None, redis_type='list')

    return f'*2\r\n${len(key)}\r\n{key}\r\n${len(elmt)}\r\n{elmt}\r\n'.encode()


def run_lrange(args: list[str]) -> bytes:
    key = args[0]
    store_entry = STORE.get(key)

    # Return an empty array if the key doesn't exist.
    if store_entry is None:
        return b'*0\r\n'  
    
    lst = store_entry.value
    lst_size = len(lst)

    # Convert negative indexes to their positive equivalents using the list length as the offset.
    # Clamp any out-of-bounds negative index to 0.
    start, stop = int(args[1]), int(args[2])
    start = max(0, lst_size + start if start < 0 else start)
    stop = max(0, lst_size + stop if stop < 0 else stop)

    # Clamp stop to the last valid index if it exceeds the list length.
    if stop >= lst_size: 
        stop = lst_size - 1

    # Return an empty array if start is out of bounds or greater than stop.
    if start >= lst_size or start > stop: 
        return b'*0\r\n'  

    sliced = lst[start:stop+1]
    resp_array = [f'*{len(sliced)}\r\n']

    for elmt in sliced:
        resp_array.append(f'${len(elmt)}\r\n')
        resp_array.append(f'{elmt}\r\n')
    
    # Join the RESP parts into a single continuous bytes object before sending over the socket.
    return ''.join(resp_array).encode()


def run_llen(args: list[str]) -> bytes:
    key = args[0]
    store_entry = STORE.get(key)
    lst = store_entry.value if store_entry is not None else []

    if not lst:
        return b':0\r\n'
    
    return f':{len(lst)}\r\n'.encode()


def parse_stream_id(stream_id: str) -> tuple[int, int | str]:
    if stream_id == '*':
        return int(time() * 1000), '*'
    
    # No sequence number provided
    if '-' not in stream_id:
        stream_id = stream_id + '-0'

    parts = stream_id.split('-')
    ms_time = int(parts[0])
    seq_num = parts[1] if parts[1] == '*' else int(parts[1])

    return ms_time, seq_num


def run_xadd(args: list[str]) -> bytes: 
    stream_id = args[1]
    kv_pairs = args[2:]
    keys = kv_pairs[0::2]
    values = kv_pairs[1::2]
    fields = dict(zip(keys, values))  # Refactored version (below)

    # fields = {}
    # for i in range(len(keys)): 
    #     fields[keys[i]] = values[i]

    # print('WAITERS —', WAITERS)  # Debug

    ms_time, seq_num = parse_stream_id(stream_id)

    if ms_time == 0 and seq_num == 0:  
        return b'-ERR The ID specified in XADD must be greater than 0-0\r\n'
    
    key = args[0]
    store_entry = STORE.get(key)

    if store_entry is None:
        if seq_num == '*': 
            seq_num = 1 if ms_time == 0 else 0

        stream_id = f'{ms_time}-{seq_num}'
        stream = [(stream_id, fields)]
        STORE[key] = StoreEntry(value=stream, expiry_time=None, redis_type='stream')
    else:
        last_stream_entry = store_entry.value[-1]  
        last_entry_id_parts = last_stream_entry[0].split('-')
        last_entry_ms_time = int(last_entry_id_parts[0])
        last_entry_seq_num = int(last_entry_id_parts[1])

        if seq_num == '*': 
            if ms_time == 0: 
                seq_num = 1
            elif ms_time == last_entry_ms_time: 
                seq_num = last_entry_seq_num + 1
            elif ms_time != 0 and ms_time != last_entry_ms_time:
                seq_num = 0
        
        assert isinstance(seq_num, int), "seq_num should be an int, not '*'"

        if ms_time < last_entry_ms_time or (ms_time == last_entry_ms_time and seq_num <= last_entry_seq_num):
            return b'-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n'
        
        stream = store_entry.value
        stream_id = f'{ms_time}-{seq_num}'
        stream.append((stream_id, fields))
        STORE[key] = StoreEntry(value=stream, expiry_time=None, redis_type='stream')

    event_list = WAITERS.get(key, [])  # Notify `XRANGE` that an entry has been added.
    if event_list:
        # print('if event_list executed')  # Debug
        event_list[0].set()  # Index 0 contains the longest-waiting client request. 

    return f'${len(stream_id)}\r\n{stream_id}\r\n'.encode()


def build_entries_array(args: list[tuple[str, list[str]]]) -> list[str]: 
    entries = [f'*{len(args)}\r\n']

    for stream_id, kv_list in args:  # Unpack tuple entry
        entries.append('*2\r\n')
        entries.append(f'${len(stream_id)}\r\n{stream_id}\r\n')
        entries.append(f'*{len(kv_list)}\r\n')

        for elmt in kv_list: 
            resp_elmt = f'${len(elmt)}\r\n{elmt}\r\n'
            entries.append(resp_elmt)

    return entries


def run_xrange(args: list[str]) -> bytes:
    key = args[0]
    store_entry = STORE.get(key)

    if store_entry is None:
        return b'*0\r\n'

    # '-' as the start ID indicates the beginning of the stream; default to the minimum possible ID.
    if args[1] == '-': 
        start_id_ms_time, start_id_seq_num = 0, 0
    else: 
        start_id_ms_time, start_id_seq_num = parse_stream_id(args[1])  # If start ID has no sequence number, default to 0.

    # '+' as the end ID indicates the end of the stream; default to the maximum possible int value.
    if args[2] == '+':
        end_id_ms_time, end_id_seq_num = sys.maxsize, sys.maxsize
    else: 
        end_id_ms_time, end_id_seq_num = parse_stream_id(args[2])  # If end ID has no sequence number, default to the maximum possible int value.
        if '-' not in args[2]:
            end_id_seq_num = sys.maxsize
    
    stream = store_entry.value  
    matches = []
    for entry in stream: 
        entry_id_ms_time, entry_id_seq_num = parse_stream_id(entry[0])
        entry_fields = entry[1] 

        if (start_id_ms_time, start_id_seq_num) <= (entry_id_ms_time, entry_id_seq_num) <= (end_id_ms_time, end_id_seq_num):
            kv_list = []
            for field_key, value in entry_fields.items():
                kv_list.append(field_key)
                kv_list.append(value)
            matches.append((entry[0], kv_list))  
        
    resp_array = build_entries_array(matches)

    return ''.join(resp_array).encode()


def get_entry_matches(stream: list[tuple[str, dict[str, str]]], stream_id: str) -> list[tuple[str, list[str]]]:
    # XREAD is exclusive; entries with this ID are not included in the result.
    start_ms_time, start_seq_num = parse_stream_id(stream_id)

    matches = []
    for entry in stream: 
        entry_id_ms_time, entry_id_seq_num = parse_stream_id(entry[0])
        entry_fields = entry[1] 

        if (entry_id_ms_time, entry_id_seq_num) > (start_ms_time, start_seq_num):
            kv_list = []
            for field_key, value in entry_fields.items():
                kv_list.append(field_key)
                kv_list.append(value)
            matches.append((entry[0], kv_list))

    return matches


async def run_xread(args: list[str]) -> bytes:
    timeout = None
    if 'block' in args:
        stream_args = args[3:]  # Skip 'BLOCK', timeout, and 'STREAMS'
        mid = len(stream_args) // 2
        keys = stream_args[:mid]
        stream_ids = stream_args[mid:]
        timeout = float(args[1])
    else: 
        stream_args = args[1:]  # Skip 'STREAMS'
        mid = len(stream_args) // 2
        keys = stream_args[:mid]
        stream_ids = stream_args[mid:]

    resp_array = [f'*{len(keys)}\r\n']  # 'keys' indicates the number of streams. 
    
    for i, key in enumerate(keys): 
        store_entry = STORE.get(key)

        if store_entry is None:
            return b'*0\r\n'
        
        matches = get_entry_matches(store_entry.value, stream_ids[i])  # Parameters (stream, stream ID)
        # print('args:', args)  # Debug
        # print('matches:', matches)  # Debug

        if 'block' in args:
            if not matches: 
                event = asyncio.Event()
                event_list = WAITERS.get(key, [])
                event_list.append(event)
                WAITERS[key] = event_list
                # print('xread event stored', WAITERS)  # Debug
                
                timeout = None if timeout == 0 else timeout
                try:
                    await asyncio.wait_for(event.wait(), timeout)

                    store_entry = STORE.get(key)  # Re-fetch the stream after one or more entries has been added.
                    if store_entry is None:
                        return b'*0\r\n'
                    
                    matches = get_entry_matches(store_entry.value, stream_ids[i])
                    entries = build_entries_array(matches)
                    
                    resp_array.append('*2\r\n')  # Each stream is a 2-element array
                    resp_array.append(f'${len(key)}\r\n{key}\r\n')  # Stream key
                    resp_array.extend(entries)    # RESP array (already includes its own array header)

                    event_list = WAITERS.get(key, [])
                    if event_list:
                        event_list.pop(0)  # Remove the handled event. 
                        WAITERS[key] = event_list  # Update the events list and store it.

                    return ''.join(resp_array).encode()
                except asyncio.TimeoutError:
                    return b'*-1\r\n'
            else:
                entries = build_entries_array(matches)
                resp_array.append('*2\r\n')  # Each stream is a 2-element array
                resp_array.append(f'${len(key)}\r\n{key}\r\n')  # Stream key
                resp_array.extend(entries)    # RESP array (already includes its own array header)
                return ''.join(resp_array).encode()

        entries = build_entries_array(matches)
        resp_array.append('*2\r\n')  # Each stream is a 2-element array
        resp_array.append(f'${len(key)}\r\n{key}\r\n')  # Stream key
        resp_array.extend(entries)    # RESP array (already includes its own array header)

    return ''.join(resp_array).encode()


COMMANDS = {
    'PING': run_ping,
    'ECHO': run_echo,
    'TYPE': run_type,
    'SET': run_set,
    'GET': run_get,
    'RPUSH': run_rpush,
    'LPUSH': run_lpush,
    'LPOP': run_lpop,
    'BLPOP': run_blpop,
    'LRANGE': run_lrange,
    'LLEN': run_llen,
    'XADD': run_xadd,
    'XRANGE': run_xrange,
    'XREAD': run_xread,
}
