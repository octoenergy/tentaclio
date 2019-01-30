from dataio.clients import s3_client

from .stream_handler import StreamURLHandler


__all__ = ["S3URLHandler"]


class S3URLHandler(StreamURLHandler):
    """Handler for opening writers and readers for S3 buckets."""

    def __init__(self):
        super().__init__(s3_client.S3Client)
