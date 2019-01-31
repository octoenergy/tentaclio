import abc
import io
from typing import Any

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

    @abc.abstractmethod
    def get(self, **params) -> protocols.ReaderClosable:
        ...

    @abc.abstractmethod
    def put(self, file_obj: protocols.Writer, **params) -> None:
        ...


class StreamClientWriter(object):
    def __init__(self, client: StreamClient, buffer_factory=io.BytesIO):
        self.buffer = buffer_factory()
        self.client = client

    def write(self, contents: Any) -> int:
        return self.buffer.write(contents)

    def close(self) -> None:
        """Flush and close the writer"""
        self.buffer.seek(0)
        with self.client:
            self.client.put(self.buffer)
        self.buffer.close()


class StreamClientReader(object):
    buff: protocols.ReaderClosable

    def __init__(self, client: StreamClient):
        self.client = client
        self._load()

    def _load(self):
        with self.client:
            self.buffer = self.client.get()

    def read(self, size: int = -1) -> Any:
        """Read the contents of the buffer"""
        return self.buffer.read(size)

    def close(self) -> None:
        """Flush and close the writer"""
        self.buffer.close()
