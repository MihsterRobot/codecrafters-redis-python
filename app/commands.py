STORE = {}


def run_ping(args: str) -> bytes:
    return b'+PONG\r\n'


def run_echo(args: str) -> bytes:
    return f'${len(args)}\r\n{args}\r\n'.encode()


def run_set(args: str) -> str:
    key = args[0]
    value = args[1]
    STORE[key] = value
    return '+OK\r\n'


def run_get(args: str) -> bytes:
    value = STORE[args[0]]
    if value is None: 
        return b'$-1\r\n'
    return f'*{len(value)}\r\n{value}\r\n'.encode()


COMMANDS = {
    'PING': run_ping,
    'ECHO': run_echo,
    'SET': run_set,
    'GET': run_get
}
