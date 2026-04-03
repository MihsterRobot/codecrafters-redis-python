import time

STORE = {}


def run_ping(args: list[str]) -> bytes:
    return b'+PONG\r\n'


def run_echo(args: list[str]) -> bytes:
    return f'${len(args[0])}\r\n{args[0]}\r\n'.encode()


def run_set(args: list[str]) -> bytes:
    expiry_time = None
    if 'PX' in args: 
        expiry_time = time.time() + (int(args[3]) / 1000)
    elif 'EX' in args: 
        expiry_time = time.time() + int(args[3])

    key, value = args[0], (args[1], expiry_time)
    STORE[key] = value
    return b'+OK\r\n'


def run_get(args: list[str]) -> bytes:
    value = STORE.get(args[0])
    if value is None: 
        return b'$-1\r\n'
    if value[1] is not None and time.time() > value[1]: 
        return b'$-1\r\n'
    return f'${len(value[0])}\r\n{value[0]}\r\n'.encode()


COMMANDS = {
    'PING': run_ping,
    'ECHO': run_echo,
    'SET': run_set,
    'GET': run_get
}
