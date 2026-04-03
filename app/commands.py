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

    if entry is None:
        return b'*0\r\n'  
    lst = entry.value

    start, stop = int(args[1]), int(args[2])
    if start > stop or start >= len(lst): 
        return b'*0\r\n'  
    start = len(lst) - abs(start) if start < 0 else start
    stop = len(lst) - abs(stop) if stop < 0 else stop

    resp_lst = [f'*{len(lst[start:stop+1])}\r\n']
    for elmt in lst[start:stop+1]:
        resp_lst.append(f'${len(elmt)}\r\n')
        resp_lst.append(f'{elmt}\r\n')
    
    return ''.join(resp_lst).encode()


COMMANDS = {
    'PING': run_ping,
    'ECHO': run_echo,
    'SET': run_set,
    'GET': run_get,
    'RPUSH': run_rpush,
    'LRANGE': run_lrange
}
