"""
General note on streams and writers/resaders.
We are using byte streams (writers and readers that only operate with byte arrays)
as cannonical format to avoid a huge mess in string vs bytes duality.

We still allow client code open/write string data if they want to.
But that the actual data is always send to the under lying client (ftp, s3...)
as bytes.

One of the reasons is that S3 only supports streams as binary data, which is not fully pythonic
(albeit problably more sane).

In order to offer client code the possibility to write csv/parquet files into s3
buckets without having to worry about the underlying implementations,
We hide which stream flavour (string, bytes) is
used and only exposes the bytes versions to the S3Client.

This is down by using TextIOWrapper when text is needed by the client code and exposes
the inner buffer to the underlying client.
"""
import abc
import io
from typing import IO, Any

from dataio import protocols

from . import base_client

__all__ = [
    "StreamClient",
    "StreamClientReader",
    "StreamClientWriter",
    "StringToBytesClientWriter",
    "StringToBytesClientReader",
]


class StreamClient(base_client.BaseClient):
    """
    Interface for stream-based connections
    """

    def __enter__(self) -> "StreamClient":
        self.conn = self.connect()
        return self

    # Stream methods

    @abc.abstractmethod
    def get(self, writer: protocols.ByteWriter, **params) -> None:
        ...

    @abc.abstractmethod
    def put(self, reader: protocols.ByteReader, **params) -> None:
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


class StringToBytesClientReader(StreamClientReader):
    """String based stream reader that uses a byte buffer under the hood.

    This is used to allow clients (s3,ftp...) just use bytes while the code
    using this reader read data as strings.
    This is similar to how python itself treats text/binary files.
    """

    inner_buffer: io.BytesIO

    def __init__(self, client: StreamClient):
        self.inner_buffer = io.BytesIO()
        super().__init__(client, io.TextIOWrapper(self.inner_buffer, encoding="utf-8"))

    def _load(self):
        # interacts with the client in terms of bytes
        with self.client:
            self.client.get(self.inner_buffer)
        self.buffer.seek(0)


class StringToBytesClientWriter(StreamClientWriter):
    """String based stream reader that uses a byte buffer under the hood.

    This is used to allow clients (s3,ftp...) just use bytes while the code
    using this writer can write data as strings.
    This is similar to how python itself treats text/binary files.
    """

    inner_buffer: io.BytesIO

    def __init__(self, client: StreamClient):
        self.inner_buffer = io.BytesIO()
        super().__init__(client, io.TextIOWrapper(self.inner_buffer, encoding="utf-8"))

    def _flush(self) -> None:
        """Flush and close the writer."""
        self.buffer.seek(0)
        # interacts with the client in terms of bytes
        with self.client:
            self.client.put(self.inner_buffer)
