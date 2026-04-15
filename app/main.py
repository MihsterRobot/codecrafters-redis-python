import sys
import socket  # noqa: F401
import asyncio
import inspect

from . import resp as r
from . import commands as c


async def run_cmd(name: str, arg: list[str]) -> bytes:
        handler = c.COMMANDS[name]
        result = handler(arg)

        # Await the result if the handler is a coroutine (e.g. BLPOP, XREAD).
        if inspect.iscoroutine(result):
            result = await result

        return result


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    in_transaction = False  # Track whether the client has an active transaction opened with MULTI.
    queued_cmds = []

    # This coroutine is called automatically by the event loop each time a new
    # client connects. asyncio runs multiple instances of it concurrently,
    # one per connected client, without blocking the others.
    while True:
        # Pause this coroutine and yield control to the event loop until up to
        # 1024 bytes arrive from the client. Other handle_client instances can
        # run while this one waits.
        request = await reader.read(1024)

        # An empty bytes object signals that the client has closed the connection.
        # Close the writer, wait for the connection to fully flush and release,
        # then exit the loop to clean up this client's coroutine.
        if request == b'':
            writer.close()
            await writer.wait_closed()
            break

        cmd_name, args = r.parse_resp(request)

        # MULTI and EXEC are handled here rather than in COMMANDS since they
        # require access to the per-connection transaction state.
        if cmd_name == 'MULTI':
            if in_transaction:
                writer.write(b'-ERR MULTI calls can not be nested\r\n')
                continue

            in_transaction = True
            writer.write(b'+OK\r\n')
            continue
        elif cmd_name == 'EXEC':
            # Return an error if MULTI was never called.
            if not in_transaction: 
                writer.write(b'-ERR EXEC without MULTI\r\n')
                continue

            # Return an empty array if no commands were queued.
            if not queued_cmds:
                writer.write(b'*0\r\n')
                in_transaction = False
                continue

            resp_array = [f'*{len(queued_cmds)}\r\n'.encode()]
            for name, arg in queued_cmds:
                result = await run_cmd(name, arg)
                resp_array.append(result)

            writer.write(b''.join(resp_array))
            queued_cmds = []
            in_transaction = False
            continue
        elif cmd_name == 'DISCARD':
            if not in_transaction:
                writer.write(b'-ERR DISCARD without MULTI\r\n')
                continue

            queued_cmds = []
            writer.write(b'+OK\r\n')
            in_transaction = False
            continue
        elif in_transaction:
            queued_cmds.append((cmd_name, args))
            writer.write(b'+QUEUED\r\n')
            continue
        
        if cmd_name in c.COMMANDS: 
            result = await run_cmd(cmd_name, args)
            writer.write(result)

            # The master server sends a snapshot of its current state to the replica as an RDB file. 
            if cmd_name == 'PSYNC':
                c.REPLICA_WRITERS.append(writer) 
                rdb_file = bytes.fromhex('524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2')
                writer.write(f'${len(rdb_file)}\r\n'.encode() + rdb_file)

            if cmd_name in c.WRITE_COMMANDS:
                for rpl_writer in c.REPLICA_WRITERS:
                    rpl_writer.write(request)
                    await rpl_writer.drain()


async def main() -> None:
    # Start a TCP server on the specified port, defaulting to 6379 if not provided.
    # If running as a replica, perform the replication handshake with the master before accepting connections.
    # handle_client is passed as a callback; the event loop calls it with a
    # (reader, writer) pair each time a new client connects.
    cmd_line_args = sys.argv
    port = 6379

    if '--port' in cmd_line_args:
        port = int(cmd_line_args[cmd_line_args.index('--port') + 1])

    c.SERVER_INFO['role'] = 'slave' if '--replicaof' in cmd_line_args else 'master'

    if '--replicaof' in cmd_line_args:
        idx = cmd_line_args.index('--replicaof')
        master_addr_parts = cmd_line_args[idx + 1].split()
        master_host = master_addr_parts[0]
        master_port = int(master_addr_parts[1])

        # Open a TCP connection to the master server to initiate the replication handshake. 
        reader, writer = await asyncio.open_connection(master_host, master_port)

        # drain() ensures the write buffer is flushed before awaiting a response.
        # Without it, data may sit in asyncio's internal buffer due to Nagle's algorithm
        # or delayed transmission, causing both sides to wait on each other indefinitely.
        writer.write(b'*1\r\n$4\r\nPING\r\n')  # The replica server begins the handshake by sending a PING command to the master server.
        await writer.drain()
        await reader.read(1024)  # Wait for +PONG response from the master server. 
        
        # The REPLCONF command is used to configure a connected replica. 
        writer.write(f'*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(port))}\r\n{port}\r\n'.encode())  # Inform the master of the replica's listening port.
        writer.write(b'*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n')  # Inform the master of the replica's capabilities (supports PSYNC2 protocol).
        await writer.drain()
        await reader.read(1024)  # Wait for +OK response to first REPLCONF. 
        await reader.read(1024)  # Wait for +OK response to second REPLCONF. 

        # The PSYNC command is used to synchronize the state of the replica with the master.
        writer.write(b'*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n')
        await writer.drain()
        await reader.read(1024)  # Wait for +FULLRESYNC response.

    # Start the TCP server and begin accepting client connections.
    server = await asyncio.start_server(handle_client, 'localhost', port)
    
    # Run the event loop indefinitely, accepting and handling client connections.
    await server.serve_forever()


if __name__ == '__main__':
    # Start the event loop and run main() indefinitely.
    # Handles cleanup when the process is interrupted.
    asyncio.run(main())
