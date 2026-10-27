import asyncio
import socket
import logging
from dataclasses import dataclass
from collections.abc import Awaitable, Callable
from typing import Iterable, Optional

MAX_HEADER_LEN = 1024 * 8

@dataclass
class BodyReader:
    length: int
    read: Callable[[], Awaitable[bytes]]

@dataclass
class HTTPReq:
    method: str
    uri: bytes
    version: str
    headers: list[bytes]

@dataclass
class HTTPRes:
    code: int
    headers: list[bytes]
    body: BodyReader

class HTTPError(Exception):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code
        self.message = message

class MyBuffer:
    def __init__(self, initial_cap=4096):
        self.buffer = bytearray(initial_cap)
        self.bytes_used = 0

    def get_receive_slice(self):
        return memoryview(self.buffer)[self.bytes_used:]

    def feed(self, n_bytes) -> None:
        self.bytes_used += n_bytes

    def pop_bytes(self, n: int) -> bytes:
        if n > self.bytes_used:
            raise ValueError("Cannot pop more bytes than available in buffer")
    
        chunk = bytes(self.buffer[:n])
        self.buffer[:self.bytes_used - n] = self.buffer[n:self.bytes_used]
        self.bytes_used -= n
        return chunk

    def cut_message(self) -> HTTPReq|None:
        """ Gets the HTTPReq in buffer if one is already read

        """
        idx = self.buffer.find(b'\r\n\r\n', 0, self.bytes_used)
        if idx < 0:
            if len(self.buffer) >= MAX_HEADER_LEN:
                raise HTTPError(413, 'header is too large')
            return None

        end = idx + 4
        msg = self._parse_http_req(end)

        self.buffer[:self.bytes_used-end] = self.buffer[end:self.bytes_used]
        self.bytes_used -= end
        return msg

    def _parse_http_req(self, end_idx: int):
        req_bytes = self.buffer[:end_idx]
        lines = req_bytes.splitlines()
        method, uri, version = lines[0].split()

        headers = []
        for i in range(1, len(lines)-1):
            h = bytes(lines[i])
            if not self._valid_header(h):
                raise HTTPError(400, 'bad fields in' + h.decode())
            headers.append(h)

        return HTTPReq(method=method.decode().upper(), uri=uri, version=version.decode().upper(), headers=headers)

    def _valid_header(self, line: bytes) -> bool:
        if b':' not in line:
            return False

        name, _, _ = line.partition(b':')

        # Field name cannot be empty and must not contain spaces or control characters
        if not name or any(b <= 32 or b == 127 for b in name):
            return False

        return True

def field_get(headers: Iterable[bytes], key: str) -> Optional[bytes]:
    target_key = key.lower().encode('ascii', errors='ignore')

    for line in headers:
        line = line.strip()
        if not line or b':' not in line:
            continue

        name, value = line.split(b':', 1)

        if name.strip().lower() == target_key:
            return value.strip()

    return None

def reader_from_req(client_socket: socket.socket, buf: MyBuffer, req: HTTPReq) -> BodyReader:
    body_len = -1
    contentLen = field_get(req.headers, 'Content-Length')
    if contentLen:
        try:
            body_len = int(contentLen.decode())
        except (ValueError, TypeError):
            raise HTTPError(400, 'bad Content-Length')

    body_allowed = not (req.method.upper() in ['GET', 'HEAD'])
    transfer_encoding = field_get(req.headers, 'Transfer-Encoding')
    chunked = (transfer_encoding is not None) and str(transfer_encoding).upper() == 'CHUNKED'

    if not body_allowed and (body_len > 0 or chunked):
        raise HTTPError(400, 'HTTP body not allowed')

    if not body_allowed:
        body_len = 0

    if body_len >= 0: # with Content-Length
        return reader_from_conn_length(client_socket, buf, body_len)
    elif chunked:
        raise HTTPError(501, 'TODO')
    else: # read the rest of the connection
        raise HTTPError(501, 'TODO')

def reader_from_conn_length(client_socket: socket.socket, buf: MyBuffer, remain: int) -> BodyReader:
    loop = asyncio.get_running_loop()
    async def reader():
        nonlocal remain
        if remain == 0:
            return b''
        if buf.bytes_used == 0:
            n_bytes = await loop.sock_recv_into(client_socket, buf.get_receive_slice())
            buf.feed(n_bytes)

            if n_bytes == 0:
                raise Exception('Unexpected EOF from HTTP body')

        consume = min(buf.bytes_used, remain)
        remain -= consume
        return buf.pop_bytes(consume)
    return BodyReader(remain, reader)

def reader_from_memory(data: bytes) -> BodyReader:
    done = False

    async def read_chunk() -> bytes:
        nonlocal done
        if done:
            return b""
        done = True
        return data

    return BodyReader(length=len(data), read=read_chunk)

STATUS_REASONS = {
    200: b"OK",
    201: b"Created",
    400: b"Bad Request",
    404: b"Not Found",
    500: b"Internal Server Error",
}

def encode_http_resp(resp: HTTPRes) -> bytes:
    reason = STATUS_REASONS.get(resp.code, b"OK")
    status_line = b"HTTP/1.1 " + str(resp.code).encode('ascii') + b" " + reason + b"\r\n"
    header_block = b"".join(h + b"\r\n" for h in resp.headers)
    return status_line + header_block + b"\r\n"

async def handle_req(req: HTTPReq, body_reader: BodyReader) -> HTTPRes:
    print(req)
    res_body = reader_from_memory(b"hello world.\n")
    if req.uri.decode().lower() == '/echo':
        res_body = body_reader
    return HTTPRes(code=200, headers=[b"Server: my_first_http_server"], body=res_body)


async def write_http_res(client_socket: socket.socket, res: HTTPRes) -> None:
    if res.body.length < 0:
        raise Exception('TODO: chunked encoding')
    
    assert not field_get(res.headers, 'Content-Length')
    res.headers.append(bytes(f"Content-Length: {res.body.length}", 'utf-8'))
    loop = asyncio.get_running_loop()
    await loop.sock_sendall(client_socket, encode_http_resp(res))

    while True:
        data = await res.body.read()
        if len(data) == 0:
            break
        await loop.sock_sendall(client_socket, data)

async def handle_client(loop: asyncio.AbstractEventLoop, client_socket: socket.socket):
    buffer = MyBuffer()
    try:
        while True:
            req = buffer.cut_message()
            
            if not req:
                n_bytes = await loop.sock_recv_into(client_socket, buffer.get_receive_slice())
                buffer.feed(n_bytes)

                if n_bytes == 0 and buffer.bytes_used == 0: # socket closed & no pendding req
                    return

                if n_bytes == 0:
                    raise HTTPError(code=400, message='Unexpected EOF.')
                # got some data try again
                continue

            # we have a complete req to handle!!!
            req_body: BodyReader = reader_from_req(client_socket, buffer, req)
            res = await handle_req(req, req_body)
            await write_http_res(client_socket, res)

            if req.version in ['1.0', 'HTTP/1.0']:
                return

            # make sure that the req body is completely consumed
            while len(await req_body.read()) > 0:
                pass
    except HTTPError as ex:
        logging.exception(ex)
        res = HTTPRes(ex.code, [], reader_from_memory(ex.message.encode()))
        try:
            await write_http_res(client_socket, res)
        except:
            pass # ignore
    except Exception as ex:
        logging.exception(ex)
    finally:
        client_socket.close()

async def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    address = ('127.0.0.1', 1234)
    server_socket.setblocking(False)
    server_socket.bind(address)
    server_socket.listen()
    print(f'listening @{address}')

    loop = asyncio.get_running_loop()

    while True:
        client_socket, client_address = await loop.sock_accept(server_socket)
        print(f'connected: {client_address}')
        client_socket.setblocking(False)
        client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1) # avoid buffering at the OS level

        asyncio.create_task(handle_client(loop, client_socket))


if __name__ == "__main__":
    asyncio.run(main())
