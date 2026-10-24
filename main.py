import asyncio
import socket
import logging

class MyBuffer:
    def __init__(self, initial_cap=4096):
        self.buffer = bytearray(initial_cap)
        self.view = memoryview(self.buffer)
        self.bytes_used = 0

    def get_receive_slice(self):
        return self.view[self.bytes_used:]

    def feed(self, n_bytes):
        self.bytes_used += n_bytes

    def read_lines(self):
        start = 0

        while True:
            nl_index = self.buffer.find(b'\n', start, self.bytes_used)
            if nl_index == -1:
                break

            yield self.view[start:nl_index].tobytes()
            start = nl_index + 1

        if start > 0:
            if start < self.bytes_used:
                self.buffer[:self.bytes_used-start] = self.buffer[start:self.bytes_used]
            self.bytes_used -= start

async def handle_client(loop: asyncio.AbstractEventLoop, client_socket: socket.socket):
    buffer = MyBuffer()
    finished = False

    try:
        while not finished:
            n_bytes = await loop.sock_recv_into(client_socket, buffer.get_receive_slice())
            if n_bytes == 0: # socket closed
                break
            buffer.feed(n_bytes)

            for data in buffer.read_lines():
                print(f'got data: {data}')
                if data == b'quit':
                    await loop.sock_sendall(client_socket, b'Bye.\n')
                    finished = True
                    break
                await loop.sock_sendall(client_socket, bytes(f"Echo: {data.decode('utf-8')}\n", 'utf-8'))
    except Exception as ex:
        logging.exception(ex)
    finally:
        client_socket.close()

async def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    address = ('127.0.0.1', 5000)
    server_socket.setblocking(False)
    server_socket.bind(address)
    server_socket.listen()
    print(f'listening @{address}')

    loop = asyncio.get_running_loop()

    while True:
        client_socket, client_address = await loop.sock_accept(server_socket)
        print(f'connected: {client_address}')
        client_socket.setblocking(False)

        asyncio.create_task(handle_client(loop, client_socket))


if __name__ == "__main__":
    asyncio.run(main())
