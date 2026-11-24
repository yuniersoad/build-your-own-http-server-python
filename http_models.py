from dataclasses import dataclass
from typing import Callable, Awaitable, Optional

MAX_HEADER_LEN = 1024 * 8

@dataclass
class BodyReader:
    length: int
    # returns:
    #   b'<some bytes>': if data is available ready to send
    #   None: if there is no data at the moment, but more is expected... not the most elegant but it is ok for learning purpose
    #   b'': end of data
    read: Callable[[], Awaitable[bytes|None]]
    close: Optional[Callable[[], None]] = None


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
    could_compress: bool
    body: BodyReader


class HTTPError(Exception):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


STATUS_REASONS = {
    200: b"OK",
    201: b"Created",
    400: b"Bad Request",
    404: b"Not Found",
    416: b"Requested range not satisfiable",
    500: b"Internal Server Error",
    501: b"Internal Server Error",
}