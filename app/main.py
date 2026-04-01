import socket  # noqa: F401
import asyncio


async def handle_client(reader, writer):
     while True:
        response = await reader.read(1024)  # Receive data as bytes object from the socket
        writer.write(b'+PONG\r\n')  # Send data to the socket
        if response == b'': # Close the connection and exit the loop if the response is empty
            writer.close()
            break



async def main():
    server_socket = await asyncio.start_server(handle_client, 'localhost', 6379)
    server_socket.serve_forever()


if __name__ == '__main__':
    asyncio.run(main())
