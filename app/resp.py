'''RESP2 protocol parser for decoding Redis client commands.'''

def parse_resp(data: bytes) -> tuple[str, list[str], int]:
    '''Parse the RESP-encoded input to derive its command name, arguments, and byte length. 

    Args:
        data: The RESP-encoded input.

    Returns:
        A tuple containing the command name, a list of its arguments, and the number of bytes consumed.
    '''
    crlf = b'\r\n'  # split() expects a bytes separator because 'data' is a bytes object.
    tokens = data.split(crlf)

    # Strip the leading '*' from the array marker and convert the element count to an int.
    num_elements = int(tokens[0][1:])

    # Tokens alternate between size markers (odd indexes) and values (even indexes).
    # stop accounts for the leading array marker at index 0, hence '+ 1'.
    stop = num_elements * 2 + 1

    cmd_parts = []
    for i in range(2, stop, 2):
        cmd_parts.append(tokens[i].decode())

    cmd_name = cmd_parts[0]
    args = cmd_parts[1:] if len(cmd_parts) > 1 else []

    # Rejoin consumed tokens to calculate the byte length of the parsed command.
    consumed = crlf.join(tokens[:stop]) + crlf
    bytes_consumed = len(consumed)
    
    return cmd_name, args, bytes_consumed
