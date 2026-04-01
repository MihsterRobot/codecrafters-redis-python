import socket  # noqa: F401
import asyncio


async def handle_client(reader, writer):
     while True:
        response = reader.read(1024)  # Receive data as bytes object from the socket
        await response
        writer.write(b'+PONG\r\n')  # Send data to the socket
        if response == b'': # Close the connection and exit the loop if the response is empty
            writer.close()
            break



async def main():
    server_socket = asyncio.start_server(handle_client, 'localhost', 6379)
    await server_socket


if __name__ == '__main__':
    asyncio.run(main())
