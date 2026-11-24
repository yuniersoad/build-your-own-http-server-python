from dataclasses import dataclass
import socket
from typing import Callable, Awaitable
import asyncio
from asyncio import Queue
from buffer import MyBuffer

from http_models import BodyReader

WS_DATA_TYPE_TEXT = 0x01
WS_DATA_TYPE_BINARY = 0x02

@dataclass
class WSMessage:
    type: int
    length: int # -1 if fragmented
    read: Callable[[], Awaitable[bytes]]

@dataclass
class WSFrameHeader:
    header_length: int
    payload_length: int
    fin: bool
    masked: bool
    opcode: int
    masking_key: bytes | None
    


class WSServer:
    QUEUES_CAPACITY = 10
    
    def __init__(self, loop: asyncio.AbstractEventLoop, client_socket: socket.socket, buf: MyBuffer):
        self.client_socket = client_socket
        self.buf = buf
        self.loop = loop
        self.qrecv: Queue[WSMessage] = Queue(self.QUEUES_CAPACITY)
        self.qsend: Queue[WSMessage] = Queue(self.QUEUES_CAPACITY)

        self.qsock: Queue[bytes|None] = Queue()

        loop.create_task(self._ws_server_recv())
        loop.create_task(self._ws_server_send())

        self.respBodyReader = BodyReader(length=-1, read=self.qsock.get)

    @property
    def get_resp_body_reader(self) -> BodyReader:
        return self.respBodyReader
    
    async def send(self, msg: WSMessage) -> None:
        await self.qsend.put(msg)

    async def recv(self) -> WSMessage:
        return await self.qrecv.get()

    def close() -> None:
        pass

    async def _ws_server_recv(self) -> None:
        in_fragmented_msg = False
        dataq = asyncio.Queue()
        while True:
            header = parse_ws_frame_header(self.buf)

            if header is None:
                bytes_read = await self.loop.sock_recv_into(self.client_socket, self.buf.get_receive_slice())
                self.buf.feed(bytes_read)

                if bytes == 0 and self.buf.bytes_used == 0:  # socket closed & no pendding req
                    return
                
                continue

            if not in_fragmented_msg:
                dataq = asyncio.Queue()
                msg = WSMessage(1, -1, lambda: dataq.get())
                print(msg)
                await self.qrecv.put(msg)

            in_fragmented_msg = not header.fin
            
            print(header)
            self.buf.pop_bytes(header.header_length)
            
            remain = header.payload_length
            while remain > 0:
                if self.buf.bytes_used == 0:
                    n_bytes = await self.loop.sock_recv_into(self.client_socket, self.buf.get_receive_slice())
                    self.buf.feed(n_bytes)
                    if n_bytes == 0:
                        raise Exception('Unexpected EOF in WS frame')

                consume = min(remain, self.buf.bytes_used)
                remain -= consume
                data = self.buf.pop_bytes(consume)
                if header.masked:
                    data =   bytes(byte ^ header.masking_key[i % 4] for i, byte in enumerate(data))
                await dataq.put(data)
                if not in_fragmented_msg:
                    await dataq.put(b'')


    async def _ws_server_send(self) -> None:
        pass

def parse_ws_frame_header(buf: MyBuffer) -> WSFrameHeader | None:
    data = memoryview(buf.buffer)[:buf.bytes_used]

    # Base header requires at least 2 bytes.
    if len(data) < 2:
        return None

    byte1, byte2 = data[0], data[1]

    fin = bool(byte1 & 0x80)
    rsv = byte1 & 0x70
    opcode = byte1 & 0x0F

    masked = bool(byte2 & 0x80)
    payload_len = byte2 & 0x7F

    # This implementation doesn't support extensions.
    if rsv:
        raise ValueError("Reserved bits must be zero")

    if opcode not in (0, 1, 2, 8, 9, 10):
        raise ValueError(f"Invalid WebSocket opcode: {opcode}")

    offset = 2

    # Extended payload length: 16 bits.
    if payload_len == 126:
        if len(data) < offset + 2:
            return None

        payload_len = int.from_bytes(data[offset:offset + 2], "big")
        offset += 2

        if payload_len < 126:
            raise ValueError("Non-minimal payload length encoding")

    # Extended payload length: 64 bits.
    elif payload_len == 127:
        if len(data) < offset + 8:
            return None

        payload_len = int.from_bytes(data[offset:offset + 8], "big")
        offset += 8

        if payload_len & (1 << 63):
            raise ValueError("Invalid payload length")

        if payload_len < 65536:
            raise ValueError("Non-minimal payload length encoding")

    masking_key = None

    if masked:
        if len(data) < offset + 4:
            return None

        masking_key = bytes(data[offset:offset + 4])
        offset += 4

    # Control frames have additional constraints.
    if opcode >= 8:
        if not fin:
            raise ValueError("Control frames cannot be fragmented")
        if payload_len > 125:
            raise ValueError("Control frame payload cannot exceed 125 bytes")

    return WSFrameHeader(
        header_length=offset,
        payload_length=payload_len,
        fin=fin,
        masked=masked,
        opcode=opcode,
        masking_key=masking_key,
    )