import socket  # noqa: F401


def main():
    server_socket = socket.create_server(('localhost', 6379), reuse_port=True)
    response = None

    while True:
        connection, _ = server_socket.accept()  # Establish a connection with the client
        while True: 
            response = connection.recv(1024)  # Receive data as bytes object from the socket
            connection.sendall(b'+PONG\r\n')  # Send data to the socket
            if not response: # Exit the loop if the response if empty
                break
        connection.close()


if __name__ == '__main__':
    main()
