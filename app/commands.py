STORE = {}


def run_ping(args: list[str]) -> bytes:
    return b'+PONG\r\n'


def run_echo(args: list[str]) -> bytes:
    return f'${len(args)}\r\n{args[0]}\r\n'.encode()


def run_set(args: list[str]) -> bytes:
    key, value = args[0], args[1]
    STORE[key] = value
    return b'+OK\r\n'


def run_get(args: list[str]) -> bytes:
    value = STORE.get(args[0])
    if value is None: 
        return b'$-1\r\n'
    return f'${len(value)}\r\n{value}\r\n'.encode()


COMMANDS = {
    'PING': run_ping,
    'ECHO': run_echo,
    'SET': run_set,
    'GET': run_get
}
