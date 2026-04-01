import socket  # noqa: F401


def main():
    server_socket = socket.create_server(('localhost', 6379), reuse_port=True)
    response = None

    while True: 
        while response != "b''": 
            connection, _ = server_socket.accept() 
            connection.recv(1024)
            connection.sendall(b'+PONG\r\n')


if __name__ == '__main__':
    main()
