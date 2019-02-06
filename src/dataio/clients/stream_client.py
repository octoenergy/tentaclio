import abc
import io
from typing import Any, cast

from dataio import protocols

from . import base_client

__all__ = ["StreamClient", "StreamClientReader", "StreamClientWriter"]


class StreamClient(base_client.BaseClient):
    """
    Interface for stream-based connections
    """

    def __enter__(self) -> "StreamClient":
        self.conn = self.connect()
        return self

    # Stream methods

    @abc.abstractmethod
    def get(self, writer: protocols.Writer, **params) -> None:
        ...

    @abc.abstractmethod
    def put(self, reader: protocols.Reader, **params) -> None:
        ...


class StreamClientWriter(object):
    buffer: io.RawIOBase

    def __init__(self, client: StreamClient, buffer_factory):
        self.buffer = buffer_factory()
        self.client = client

    def write(self, contents: Any) -> int:
        return cast(int, self.buffer.write(contents))

    def close(self) -> None:
        """Flush and close the writer."""
        self.buffer.seek(0)
        with self.client:
            self.client.put(self.buffer)
        self.buffer.close()


class StreamClientReader(object):
    buffer: io.RawIOBase

    def __init__(self, client: StreamClient, buffer_factory):
        self.client = client
        self.buffer = buffer_factory()
        self._load()

    def _load(self):
        with self.client:
            self.client.get(self.buffer)
        self.buffer.seek(0)

    def read(self, size: int = -1) -> Any:
        """Read the contents of the buffer."""
        return self.buffer.read(size)

    def close(self) -> None:
        """Flush and close the writer"""
        self.buffer.close()
