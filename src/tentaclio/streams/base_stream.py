"""General note on streams and writers/readers.

We are using byte streams (writers and readers that only operate with byte arrays)
as canonical format to avoid a huge mess in string vs bytes duality.

We still allow client code open/write string data if they want to.
But that the actual data is always send to the under lying client (ftp, s3...) as bytes.

One of the reasons is that S3 only supports streams as binary data, which is not fully pythonic
(albeit being probably more sane).

In order to offer client code the possibility to write csv/parquet files into s3 buckets without
having to worry about the underlying implementations. We hide which stream flavour (string, bytes)
is used and only exposes the bytes versions to the S3Client.

This is down by using TextIOWrapper when text is needed by the client code and exposes the inner
buffer to the underlying client.
"""
import io
from typing import IO, Any

from tentaclio.clients import base_client


class StreamBaseIO:
    """Base class for IO streams that interact with StreamClients.

    The extra methods included are to ensure interoperability with loosely defined,
    third party libraries such as `pyarrow`.

    https://docs.python.org/3/library/io.html
    """

    buffer: IO

    def __init__(self, buffer: IO):
        """Create a StreamBase which wraps the provided buffer."""
        self.buffer = buffer

    @property
    def closed(self):
        """Tell if the resource is closed."""
        self.buffer.closed

    def __iter__(self):
        """Return the resource as an iterable.

        For pandas compatibility
        https://github.com/pandas-dev/pandas/blob/master/pandas/core/dtypes/inference.py#L194
        """
        # for pandas compatibility
        return self.buffer.__iter__()

    def seek(self, *args, **kargs):
        """Change the stream position to the given byte offset."""
        return self.buffer.seek(*args, **kargs)

    def tell(self, *args, **kargs):
        """Return the current stream position."""
        return self.buffer.tell(*args, **kargs)


class StreamClientWriter(StreamBaseIO):
    """Offer stream like access to underlying client.

    Offer a IO stream interface to clients while maintaining the
    connection atomic.
    """

    def __init__(self, client: base_client.StreamClient, buffer: IO):
        """Create a new writer based on a stream client and a buffer."""
        super().__init__(buffer)
        self.client = client

    def write(self, contents: Any) -> int:
        """Write the contents to the underlying buffer."""
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

    def __init__(self, client: base_client.StreamClient, buffer: IO):
        """Create a reader that will read from the given client to the passed buffer."""
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

    def readline(self, size: int = -1):
        """Read and return one line from the buffer."""
        # Additional method required for unpickling Python objects
        return self.buffer.readline(size)

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

    def __init__(self, client: base_client.StreamClient):
        """Create a byte based reader that will read from the given client."""
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

    def __init__(self, client: base_client.StreamClient):
        """Create a byte based write that will read from the given client."""
        self.inner_buffer = io.BytesIO()
        super().__init__(client, io.TextIOWrapper(self.inner_buffer, encoding="utf-8"))

    def _flush(self) -> None:
        """Flush and close the writer."""
        self.buffer.seek(0)
        # interacts with the client in terms of bytes
        with self.client:
            self.client.put(self.inner_buffer)
