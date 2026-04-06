import time
from typing import NamedTuple

STORE = {}


class StoreEntry(NamedTuple): 
    value: str | list[str]
    expiry_time: float | None


def run_ping(args: list[str]) -> bytes:
    return b'+PONG\r\n'


def run_echo(args: list[str]) -> bytes:
    return f'${len(args[0])}\r\n{args[0]}\r\n'.encode()


def run_set(args: list[str]) -> bytes:
    key = args[0]
    expiry_time = None

    if 'PX' in args: 
        expiry_time = time.time() + (int(args[3]) / 1000)
    elif 'EX' in args: 
        expiry_time = time.time() + int(args[3])

    STORE[key] = StoreEntry(value=args[1], expiry_time=expiry_time)

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
    STORE[key] = StoreEntry(value=lst, expiry_time=None)

    return f':{len(lst)}\r\n'.encode()


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


def run_lpush(args: list[str]) -> bytes: 
    key, elements = args[0], args[1:]
    entry = STORE.get(key)
    lst = entry.value if entry is not None else []

    # Reverse elements, prepend them to the existing list, and store the result. 
    lst = elements[::-1] + lst  
    STORE[key] = StoreEntry(value=lst, expiry_time=None)

    return f':{len(lst)}\r\n'.encode()


def run_llen(args: list[str]) -> bytes:
    key = args[0]
    entry = STORE.get(key)
    lst = entry.value if entry is not None else []

    if not lst:
        return b':0\r\n'
    
    return f':{len(lst)}\r\n'.encode()


def run_lpop(args: list[str]) -> bytes: 
    key = args[0]
    entry = STORE.get(key)
    lst = entry.value if entry is not None else []

    if not lst:  
        return b'$-1\r\n'

    if len(args) > 1:  # Only one element to pop (guard clause).
        elmt = lst.pop(0)
        return f'${len(elmt)}\r\n{elmt}\r\n'.encode()

    num_elmts_to_pop = None
    num = int(args[1])

    if num > len(lst): 
        num_elmts_to_pop = len(lst) - 1
    else: 
        num_elmts_to_pop = num
    
    popped_elmts = []
    for elmt in lst[0:num_elmts_to_pop]: 
        popped_elmts.append(lst.pop(0))

    STORE[key] = StoreEntry(value=lst, expiry_time=None)

    resp_lst = [f'*{len(popped_elmts)}\r\n']
    for elmt in popped_elmts:
        resp_lst.append(f'${len(elmt)}\r\n')
        resp_lst.append(f'{elmt}\r\n')

    return ''.join(resp_lst).encode()


COMMANDS = {
    'PING': run_ping,
    'ECHO': run_echo,
    'SET': run_set,
    'GET': run_get,
    'RPUSH': run_rpush,
    'LRANGE': run_lrange,
    'LPUSH': run_lpush,
    'LLEN': run_llen,
    'LPOP': run_lpop,
}
