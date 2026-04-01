import socket  # noqa: F401


def main():
    server_socket = socket.create_server(('localhost', 6379), reuse_port=True)
    response = None

    while True: 
        while response != b'': 
            connection, _ = server_socket.accept()  # Return a new socket representing the client connection
            response = connection.recv(1024)  # Receive data as bytes object from socket
            connection.sendall(b'+PONG\r\n')  # Send data to the socket
        connection.close() 
        response = None


if __name__ == '__main__':
    main()
