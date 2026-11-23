"""asyncio test servers on 127.0.0.1 for the transport tests (`test_alpaca_http.py`).

Each server closes every client transport it accepted on `__aexit__`, before
`Server.wait_closed()` (which on Python 3.12 waits for open connections), so no socket
outlives the test (`filterwarnings=error` turns a `ResourceWarning` into a failure).
"""

import asyncio
import contextlib
import socket
from types import TracebackType
from typing import Self


def free_port() -> int:
    """A port nothing listens on: bind port 0, read it, close the socket again."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
    return port


class _Server:
    def __init__(self) -> None:
        self.port = 0
        self.connections = 0
        self._server: asyncio.Server | None = None
        self._writers: list[asyncio.StreamWriter] = []

    async def __aenter__(self) -> Self:
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        assert self._server is not None
        self._server.close()
        for writer in self._writers:
            writer.close()
        for writer in self._writers:
            with contextlib.suppress(OSError):
                await writer.wait_closed()
        await self._server.wait_closed()

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.connections += 1
        self._writers.append(writer)
        with contextlib.suppress(ConnectionError, asyncio.IncompleteReadError):
            await self.serve(reader, writer)

    async def serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        raise NotImplementedError


class SilentTls(_Server):
    """Records every byte and never answers: a TLS ClientHello waits forever, so the
    client's connection is held in setup. `hello` is set on the first bytes, `closed` on
    EOF (the client closed its socket)."""

    def __init__(self) -> None:
        super().__init__()
        self.received = b""
        self.hello = asyncio.Event()
        self.closed = asyncio.Event()

    async def serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while data := await reader.read(65536):
            self.received += data
            self.hello.set()
        self.closed.set()

    def record_types(self) -> set[int]:
        """The content types of the TLS records received (0x16 handshake, 0x17 data)."""
        types: set[int] = set()
        i = 0
        while i + 5 <= len(self.received):
            types.add(self.received[i])
            i += 5 + int.from_bytes(self.received[i + 3 : i + 5], "big")
        return types


class KeepAliveHttp(_Server):
    """A minimal HTTP/1.1 responder: answers each request on a connection with `200 {}`
    and keeps the connection open for the next one. `connections` counts accepted ones."""

    async def serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while True:
            try:
                head = await reader.readuntil(b"\r\n\r\n")
            except asyncio.IncompleteReadError:
                return  # the client closed the connection
            length = 0
            for line in head.split(b"\r\n"):
                name, _, value = line.partition(b":")
                if name.strip().lower() == b"content-length":
                    length = int(value.strip())
            if length:
                await reader.readexactly(length)
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}"
            )
            await writer.drain()
