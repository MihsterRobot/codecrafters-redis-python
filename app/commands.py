import time
import asyncio
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
        expiry_time = time.time() + (int(args[3]) / 1000)
    elif 'EX' in args: 
        expiry_time = time.time() + int(args[3])

    STORE[key] = StoreEntry(value=args[1], expiry_time=expiry_time, redis_type='string')

    return b'+OK\r\n'


def run_get(args: list[str]) -> bytes:
    key = args[0]
    entry = STORE.get(key)

    if entry is None: 
        return b'$-1\r\n'
    # If there's an expiry time and the current time exceeds it, the key is expired. 
    elif entry.expiry_time is not None and time.time() > entry.expiry_time: 
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


def run_xadd(args: list[str]) -> bytes: 
    key = args[0]
    stream_id = args[1]
    kv_pairs = args[2:]
    entry = STORE.get(key)

    keys = kv_pairs[0::2]
    values = kv_pairs[1::2]
    fields = dict(zip(keys, values))  # Refactored version (below)

    # fields = {}
    # for i in range(len(keys)): 
    #     fields[keys[i]] = values[i]

    stream_id_parts = stream_id.split('-')
    ms_time = int(stream_id_parts[0])
    seq_num = stream_id_parts[1] 
    seq_num = seq_num if seq_num == '*' else int(seq_num)

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
        last_stream_id_parts = last_stream_entry[0].split('-')
        last_stream_ms_time = int(last_stream_id_parts[0])
        last_stream_seq_num = int(last_stream_id_parts[1])

        if seq_num == '*': 
            if ms_time == 0: 
                seq_num = 1
            elif ms_time == last_stream_ms_time: 
                seq_num = last_stream_seq_num + 1
            elif ms_time != 0 and ms_time != last_stream_ms_time:
                seq_num = 0
        
        assert isinstance(seq_num, int), "seq_num should be resolved from '*' by this point"

        if ms_time < last_stream_ms_time or (ms_time == last_stream_ms_time and seq_num <= last_stream_seq_num):
            return b'-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n'
        
        stream = entry.value
        stream_id = f'{ms_time}-{seq_num}'
        stream.append((stream_id, fields))
        STORE[key] = StoreEntry(value=stream, expiry_time=None, redis_type='stream')

    return f'${len(stream_id)}\r\n{stream_id}\r\n'.encode()  


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
}
