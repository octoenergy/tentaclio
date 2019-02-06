""" S3 objects as  streams

General note. S3 only supports streams as binary data, which is not fully pythonic
(albeit problably more sane).

In order to offer client code write csv/parquet files into s3
buckets without having to worry about the underlying implementations
s3 handlers modifies the StreamClientReader/Writers.
It hides which stream flavour (string, bytes) is
used and only exposes bytes versions to the S3Client.

This is down by using TextIOWrapper when text is needed by the client code and exposes
the inner buffer to the S3 client.
"""

import io

from dataio.clients import StreamClient, StreamClientReader, StreamClientWriter, s3_client
from dataio.protocols import ReaderClosable, WriterClosable
from dataio.urls import URL

from .stream_handler import is_bytes_mode


__all__ = ["S3URLHandler"]


class S3ClientReader(StreamClientReader):
    inner_buffer: io.BytesIO

    def __init__(self, client: StreamClient):
        self.inner_buffer = io.BytesIO()
        super().__init__(client, io.TextIOWrapper(self.inner_buffer, encoding="utf-8"))

    def _load(self):
        # interacts with the client in terms of bytes
        with self.client:
            self.client.get(self.inner_buffer)
        self.buffer.seek(0)


class S3ClientWriter(StreamClientWriter):
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


class S3URLHandler:
    """Handler for opening writers and readers for S3 buckets."""

    def open_reader_for(self, url: URL, mode: str, extras: dict) -> ReaderClosable:
        """Open an stream client for reading."""
        client = s3_client.S3Client(url, **extras)
        if not is_bytes_mode(mode):
            return S3ClientReader(client)

        return StreamClientReader(client, io.BytesIO())

    def open_writer_for(self, url: URL, mode: str, extras: dict) -> WriterClosable:
        """Open an stream client writing."""
        client = s3_client.S3Client(url, **extras)
        if not is_bytes_mode(mode):
            return S3ClientWriter(client)
        return StreamClientWriter(client, io.BytesIO())
