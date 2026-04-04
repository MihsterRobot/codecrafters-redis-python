def parse_resp(data: bytes) -> tuple[str, list[str]]:
    crlf = b'\r\n'

    # Remove terminators to isolate the data and their size identifiers.
    tokens = data.split(crlf)
    print('TOKENS:', tokens)

    # Strip the leading '*' from the array marker and convert the element count to an int.
    num_elements = int(tokens[0][1:])
    print('NUM_ELEMENTS:', num_elements)

    stop = num_elements * 2 + 1
    cmd_parts = []

    for i in range(2, stop, 2):
        cmd_parts.append(tokens[i].decode())

    cmd_name = cmd_parts[0]
    print('CMD_NAME:', cmd_name)
    args = cmd_parts[1:] if len(cmd_parts) > 1 else []
    print('ARGS:', args)

    return cmd_name, args
