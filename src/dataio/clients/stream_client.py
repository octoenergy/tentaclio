import abc
from typing import IO, Any

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


class StreamBaseIO(object):
    """ Base clase for stream classes that interact with StreamClients.

    The extra methods inlcuded are to ensure interoperability with loosely defined,
    thrid party libraries such as pyarrow.
    """

    buffer: IO

    def __init__(self, buffer: IO):
        self.buffer = buffer

    @property
    def closed(self):
        self.buffer.closed

    def __iter__(self):
        # for pandas compatibility
        return self.buffer.__iter__()

    def seek(self, *args, **kargs):
        return self.buffer.seek(*args, **kargs)

    def tell(self, *args, **kargs):
        return self.buffer.tell(*args, **kargs)


class StreamClientWriter(StreamBaseIO):
    """Offer stream like access to underlying client.

    Offer a IO stream interface to clients while maintaining the
    connection atomic.
    """

    def __init__(self, client: StreamClient, buffer: IO):
        super().__init__(buffer)
        self.client = client

    def write(self, contents: Any) -> int:
        return self.buffer.write(contents)

    def close(self) -> None:
        """Flush and close the writer."""
        self._flush()
        self.buffer.close()

    def _flush(self):
        self.buffer.seek(0)
        # atomic put so we open/close connections swiftly
        with self.client:
            self.client.put(self.buffer)


class StreamClientReader(StreamBaseIO):
    """Offer stream like access to underlying client.

    Offer a IO stream interface to clients while maintaining the
    connection atomic.
    """

    buffer: IO

    def __init__(self, client: StreamClient, buffer: IO):
        super().__init__(buffer)
        self.client = client
        self._load()

    def _load(self):
        # atomic get so we open/close connections swiftly
        with self.client:
            self.client.get(self.buffer)
        self.buffer.seek(0)

    def read(self, size: int = -1):
        """Read the contents of the buffer."""
        return self.buffer.read(size)

    def close(self) -> None:
        """Close the writer."""
        self.buffer.close()
