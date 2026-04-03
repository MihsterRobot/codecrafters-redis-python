import time
from typing import NamedTuple

STORE = {}


class StoreEntry(NamedTuple): 
    value: str
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
    data = STORE.get(args[0])

    if data is None: 
        return b'$-1\r\n'
    # If there's an expiry time and the current time exceeds it, the key is expired. 
    elif data.expiry_time is not None and time.time() > data.expiry_time: 
        return b'$-1\r\n'
    
    return f'${len(data.value)}\r\n{data.value}\r\n'.encode()


COMMANDS = {
    'PING': run_ping,
    'ECHO': run_echo,
    'SET': run_set,
    'GET': run_get
}
