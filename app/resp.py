def parse_resp(data: bytes) -> tuple[str, str]:
    crlf = b'\r\n'

    # Remove terminators to isolate the data and their size identifiers.
    tokens = data.split(crlf)

    num_elements = int(tokens[0][1:])
    stop = num_elements * 2 + 1
    cmd_parts = []

    for i in range(2, stop, 2):
        cmd_parts.append(tokens[i].decode())

    cmd_name = cmd_parts[0]
    arg = ''
    if len(cmd_parts) > 1:
        arg = cmd_parts[1]

    return cmd_name, arg
