from http_models import HTTPReq, HTTPError, MAX_HEADER_LEN

class MyBuffer:
    def __init__(self, initial_cap=MAX_HEADER_LEN):
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

    def cut_message(self) -> HTTPReq | None:
        """ Gets the HTTPReq in buffer if one is already read

        """
        idx = self.buffer.find(b'\r\n\r\n', 0, self.bytes_used)
        if idx < 0:
            if self.bytes_used >= MAX_HEADER_LEN:
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

        return HTTPReq(
                method=method.decode().upper(),
                uri=uri,
                version=version.decode().upper(),
                headers=headers)

    def _valid_header(self, line: bytes) -> bool:
        if b':' not in line:
            return False

        name, _, _ = line.partition(b':')

        # Field name must not contain spaces or control characters
        if not name or any(b <= 32 or b == 127 for b in name):
            return False

        return True