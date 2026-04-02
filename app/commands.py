def run_ping(arg: str) -> bytes:
    return b'+PONG\r\n'


def run_echo(arg: str) -> bytes:
    return f'${len(arg)}\r\narg\r\n'.encode()


COMMANDS = {
    'PING': run_ping,
    'ECHO': run_echo
}
