'''RESP2 protocol parser for decoding Redis client commands.'''

def parse_resp(data: bytes) -> tuple[str, list[str]]:
    '''Parse the RESP-encoded input to derive its command name and arguments. 

    Args:
        data: The RESP-encoded input.

    Returns:
        A tuple containing the command name and a list of its arguments.
    '''
    crlf = b'\r\n'
    tokens = data.split(crlf)

    # Strip the leading '*' from the array marker and convert the element count to an int.
    num_elements = int(tokens[0][1:])

    stop = num_elements * 2 + 1
    cmd_parts = []
    for i in range(2, stop, 2):
        cmd_parts.append(tokens[i].decode())

    cmd_name = cmd_parts[0]
    args = cmd_parts[1:] if len(cmd_parts) > 1 else []
    
    return cmd_name, args
