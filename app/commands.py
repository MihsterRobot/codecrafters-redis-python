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
    entry = STORE.get(key)

    # If the key doesn't exist
    if not entry:
        return b'+none\r\n'
    
    return f'+{entry.redis_type}\r\n'.encode()


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
    entry = STORE.get(key)

    if entry is None: 
        return b'$-1\r\n'
    # If there's an expiry time and the current time exceeds it, the key is expired. 
    elif entry.expiry_time is not None and time() > entry.expiry_time: 
        return b'$-1\r\n'
    
    return f'${len(entry.value)}\r\n{entry.value}\r\n'.encode()


def run_rpush(args: list[str]) -> bytes: 
    key, elements = args[0], args[1:]
    entry = STORE.get(key)
    lst = entry.value if entry is not None else []

    lst.extend(elements)
    STORE[key] = StoreEntry(value=lst, expiry_time=None, redis_type='list')

    event_list = WAITERS.get(key, [])
    if event_list: 
        event_list[0].set()

    return f':{len(lst)}\r\n'.encode()


def run_lpush(args: list[str]) -> bytes: 
    key, elements = args[0], args[1:]
    entry = STORE.get(key)
    lst = entry.value if entry is not None else []

    # Reverse elements, prepend them to the existing list, and store the result. 
    lst = elements[::-1] + lst  
    STORE[key] = StoreEntry(value=lst, expiry_time=None, redis_type='list')

    return f':{len(lst)}\r\n'.encode()


def run_lpop(args: list[str]) -> bytes: 
    key = args[0]
    entry = STORE.get(key)
    lst = entry.value if entry is not None else []

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

    resp_lst = [f'*{len(popped_elmts)}\r\n']
    for elmt in popped_elmts:
        resp_lst.append(f'${len(elmt)}\r\n')
        resp_lst.append(f'{elmt}\r\n')

    return ''.join(resp_lst).encode()


async def run_blpop(args: list[str]) -> bytes:
    key = args[0]
    entry = STORE.get(key)
    lst = entry.value if entry is not None else []

    if not lst:
        event = asyncio.Event()
        event_list = WAITERS.get(key, [])
        event_list.append(event)
        WAITERS[key] = event_list
        
        timeout = float(args[1])
        timeout = None if timeout == 0 else timeout
        try:
            await asyncio.wait_for(event.wait(), timeout)
            lst = STORE[key].value
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
    entry = STORE.get(key)

    # Return an empty array if the key doesn't exist.
    if entry is None:
        return b'*0\r\n'  
    
    lst = entry.value
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
    resp_lst = [f'*{len(sliced)}\r\n']

    for elmt in sliced:
        resp_lst.append(f'${len(elmt)}\r\n')
        resp_lst.append(f'{elmt}\r\n')
    
    # Join the RESP parts into a single continuous bytes object before sending over the socket.
    return ''.join(resp_lst).encode()


def run_llen(args: list[str]) -> bytes:
    key = args[0]
    entry = STORE.get(key)
    lst = entry.value if entry is not None else []

    if not lst:
        return b':0\r\n'
    
    return f':{len(lst)}\r\n'.encode()


def parse_stream_id(stream_id: str) -> tuple[int, int | str]:
    if stream_id == '*':
        return int(time() * 1000), '*'
    
    # No sequence number provided
    if not '-' in stream_id:
        stream_id = stream_id + '-0'

    parts = stream_id.split('-')
    ms_time = int(parts[0])
    seq_num = parts[1] if parts[1] == '*' else int(parts[1])

    return ms_time, seq_num


def run_xadd(args: list[str]) -> bytes: 
    key = args[0]
    entry = STORE.get(key)
    stream_id = args[1]
    kv_pairs = args[2:]

    keys = kv_pairs[0::2]
    values = kv_pairs[1::2]
    fields = dict(zip(keys, values))  # Refactored version (below)

    # fields = {}
    # for i in range(len(keys)): 
    #     fields[keys[i]] = values[i]

    ms_time, seq_num = parse_stream_id(stream_id)

    if ms_time == 0 and seq_num == 0:  
        return b'-ERR The ID specified in XADD must be greater than 0-0\r\n'

    if not entry:
        if seq_num == '*': 
            seq_num = 1 if ms_time == 0 else 0

        stream_id = f'{ms_time}-{seq_num}'
        stream = [(stream_id, fields)]
        STORE[key] = StoreEntry(value=stream, expiry_time=None, redis_type='stream')
    else:
        last_stream_entry = entry.value[-1]  
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
        
        assert isinstance(seq_num, int), "seq_num should be resolved from '*' by this point"

        if ms_time < last_entry_ms_time or (ms_time == last_entry_ms_time and seq_num <= last_entry_seq_num):
            return b'-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n'
        
        stream = entry.value
        stream_id = f'{ms_time}-{seq_num}'
        stream.append((stream_id, fields))
        STORE[key] = StoreEntry(value=stream, expiry_time=None, redis_type='stream')

    return f'${len(stream_id)}\r\n{stream_id}\r\n'.encode()


def run_xrange(args: list[str]) -> bytes:
    key = args[0]
    entry = STORE.get(key)

    # If start ID has no sequence number, default to 0; if end ID has no sequence number, default to max int.
    start_id_ms_time, start_id_seq_num = parse_stream_id(args[1])
    if not '-' in args[2]:
        end_id_ms_time, end_id_seq_num = parse_stream_id(args[2])
        end_id_seq_num = sys.maxsize
    end_id_ms_time, end_id_seq_num = parse_stream_id(args[2])

    stream = entry.value  
    matches = []
    ent_list = []

    for ent in stream: 
        ent_id_ms_time, ent_id_seq_num = parse_stream_id(ent[0])
        ent_fields = ent[1]
        
        if ent_id_ms_time >= start_id_ms_time and ent_id_ms_time <= end_id_ms_time:
            if ent_id_seq_num >= start_id_seq_num and ent_id_seq_num <= end_id_seq_num:
                ent_list.append(ent[0]) # Append stream id
                kv_list = []
                for key, value in ent_fields.items():
                    kv_list.append(key)
                    kv_list.append(value)
                ent_list.append(kv_list)
                matches.append(ent_list)  # Append list to matches
        
    num_entries = f'*{len(matches)}\r\n'
    resp_entries = [num_entries]

    for ent in matches:
        stream_id = ent[0]
        kv_list = ent[1]
        ent_size = f'*{len(ent)}\r\n'
        stream_id = f'${len(stream_id)}\r\n{stream_id}\r\n'
        kv_list_size = f'*{len(kv_list)}\r\n'

        resp_entries.append(ent_size)
        resp_entries.append(stream_id)
        resp_entries.append(kv_list_size)
        

        for elmt in kv_list: 
            resp_elmt = f'${len(elmt)}\r\n{elmt}\r\n'
            resp_entries.append(resp_elmt)

    return ''.join(resp_entries).encode()


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
}
