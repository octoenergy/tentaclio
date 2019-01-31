from dataio.clients import s3_client
from dataio.clients.stream_client import StreamClientReader, StreamClientWriter
from dataio.protocols import ReaderClosable, WriterClosable
from dataio.urls import URL


__all__ = ["S3URLHandler"]


class S3URLHandler:
    """Handler for opening writers and readers for S3 buckets."""

    def open_reader_for(self, url: URL, extras: dict) -> ReaderClosable:
        """Open an s3 bucket for reading."""
        client = s3_client.S3Client(url, **extras)
        return StreamClientReader(client)

    def open_writer_for(self, url: URL, extras: dict) -> WriterClosable:
        """Open an s3 bucket for writing."""
        client = s3_client.S3Client(url, **extras)
        return StreamClientWriter(client)
