import socket  # noqa: F401
import asyncio
import inspect

from . import resp as r
from . import commands as c


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    # Track whether the client has an active transaction opened with MULTI.
    in_transaction = False

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
            in_transaction = True
            writer.write(b'+OK\r\n')
            continue
        elif cmd_name == 'EXEC':
            # Return an empty array if no commands were queued, or an error if MULTI was never called.
            if in_transaction:
                writer.write(b'*0\r\n')  
            else: 
                writer.write(b'-ERR EXEC without MULTI\r\n')
            
            in_transaction = False
            continue
        elif in_transaction:
            writer.write(b'+QUEUED\r\n')
            continue
            
        if cmd_name in c.COMMANDS: 
            handler = c.COMMANDS[cmd_name]
            result = handler(args)

            # Await the result if the handler is a coroutine (e.g. BLPOP, XREAD).
            if inspect.iscoroutine(result):
                result = await result

            writer.write(result)


async def main() -> None:
    # Start a TCP server on localhost:6379 (Redis's default port).
    # handle_client is passed as a callback; the event loop calls it with a
    # (reader, writer) pair each time a new client connects.
    server = await asyncio.start_server(handle_client, 'localhost', 6379)

    # Run the event loop indefinitely, accepting and handling client connections.
    await server.serve_forever()


if __name__ == '__main__':
    # Start the event loop and run main() indefinitely.
    # Handles cleanup when the process is interrupted.
    asyncio.run(main())
