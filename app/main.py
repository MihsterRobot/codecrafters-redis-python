'''Async TCP server implementing a subset of the Redis protocol.'''

import sys
import asyncio
import inspect

from . import resp as r
from . import commands as c

# Tracks the number of bytes received and processed from the master.
replica_repl_offset = 0


async def run_cmd(name: str, arg: list[str]) -> bytes:
    '''Look up and execute a command handler by name, awaiting it if it is a coroutine.

    Args:
        name: The name of the command to execute.
        arg: The list of arguments to pass to the handler.

    Returns:
        The RESP-encoded bytes returned by the command handler.
    '''
    handler = c.COMMANDS[name]
    result = handler(arg)
    if inspect.iscoroutine(result):
        result = await result
    return result


async def handle_replication(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    '''Continuously read and execute commands propagated from the master server.

    Reads incoming data from the replication connection, parses each RESP-encoded
    command, and executes it against the local data store without sending a response.

    Args:
        reader: The read end of the replication connection to the master.
        writer: The write end of the replication connection to the master.
    '''
    global replica_repl_offset
    while True:
        data = await reader.read(1024)
        if data == b'':
            break

        idx = data.find(b'*')
        if idx == -1:
            continue
        data = data[idx:]
        
        while data:
            cmd_name, args, bytes_consumed = r.parse_resp(data)
            replica_repl_offset += bytes_consumed
            print('cmd_name:', cmd_name, 'args:', args)

            if cmd_name == 'REPLCONF' and args[0] == 'GETACK':
                writer.write(f'*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(replica_repl_offset))}\r\n{replica_repl_offset}\r\n'.encode())
                await writer.drain()

            if cmd_name in c.COMMANDS:
                # Propagated commands run silently, which means nothing is written to the master.
                await run_cmd(cmd_name, args)  
            data = data[bytes_consumed:]


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    '''Manage the lifecycle of a single client connection, including command parsing, dispatch, and transaction state.

    Args:
        reader: The read end of the client connection.
        writer: The write end of the client connection.
    '''
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

        cmd_name, args, _ = r.parse_resp(request)

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

            # A PSYNC command identifies this connection as a replica.
            # Store the writer for propagation, then send an empty RDB file as the initial full resynchronization snapshot. 
            if cmd_name == 'PSYNC':
                c.REPLICA_WRITERS.append(writer) 
                rdb_file = bytes.fromhex('524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2')
                writer.write(f'${len(rdb_file)}\r\n'.encode() + rdb_file)
                await writer.drain()

            # Propagate 'write' commands to all connected replicas.
            if cmd_name in c.WRITE_COMMANDS:
                for repl_writer in c.REPLICA_WRITERS:
                    repl_writer.write(request)
                    await repl_writer.drain()


async def main() -> None:
    '''Entry point for the Redis server.
    
    Parses command-line arguments to determine the port and replication role.
    If running as a replica, performs the replication handshake with the master.
    Starts the TCP server and runs the event loop indefinitely.
    '''
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
        writer.write(b'*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n')  # Inform the master of the replica's capabilities (e.g., supports PSYNC2 protocol).
        await writer.drain()
        await reader.read(1024)  # Wait for +OK response to first REPLCONF. 
        await reader.read(1024)  # Wait for +OK response to second REPLCONF. 

        # The PSYNC command is used to synchronize the state of the replica with the master.
        writer.write(b'*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n')
        await writer.drain()
        await reader.read(1024)  # Wait for +FULLRESYNC response.

        # create_task schedules a coroutine to run concurrently as a background task.
        # Using 'await' would block 'main' until handle_replication is finished, which is never
        # since it's an infinite loop. create_task allows the coroutine to run in the background
        # while 'main' continues to execute start_server and serve_forever.
        asyncio.create_task(handle_replication(reader, writer))

    # Set up the TCP server with handle_client as the callback for each new connection.
    server = await asyncio.start_server(handle_client, 'localhost', port)

    # Start accepting connections and run the event loop until interrupted.
    await server.serve_forever()


if __name__ == '__main__':
    # Start the event loop and run main() indefinitely.
    # Handles cleanup when the process is interrupted.
    asyncio.run(main())
