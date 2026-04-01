import socket  # noqa: F401
import asyncio


async def handle_client(reader, writer):
     while True:
        response = connection.recv(1024)  # Receive data as bytes object from the socket
        connection.sendall(b'+PONG\r\n')  # Send data to the socket
        if response == b'': # Close the connection and exit the loop if the response is empty
            connection.close()
            break



def main():
    server_socket = asyncio.start_server(handle_client())
    

   


if __name__ == '__main__':
    main()
