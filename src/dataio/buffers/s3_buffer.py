import io
from typing import Optional

from dataio.clients import s3_client

from . import base_buffer

__all__ = ["open_s3_reader", "open_s3_writer"]


class S3Buffer(base_buffer.BaseBuffer):
    """
    Generic S3 buffer, backed by a s3 client
    """

    url: str
    bucket_name: Optional[str]
    key_name: Optional[str]
    s3_kwargs: dict
    buffer: io.BytesIO

    def __init__(
        self, url: str, bucket_name: str = None, key_name: str = None, s3_kwargs: dict = None
    ):
        self.url = url
        self.bucket_name = bucket_name
        self.key_name = key_name
        self.s3_kwargs = s3_kwargs or {}

    def __enter__(self) -> io.BytesIO:
        raise NotImplementedError()

    def __exit__(self, *args) -> None:
        raise NotImplementedError()


class open_s3_reader(S3Buffer):
    def __enter__(self) -> io.BytesIO:
        with s3_client.S3Client(self.url, **self.s3_kwargs) as client:
            self.buffer = client.get(  # type: ignore
                bucket_name=self.bucket_name, key_name=self.key_name
            )
            # Reposition to start of the stream
            self.buffer.seek(0)
            return self.buffer

    def __exit__(self, *args) -> None:
        self.buffer = io.BytesIO()


class open_s3_writer(S3Buffer):
    def __enter__(self) -> io.BytesIO:
        self.buffer = io.BytesIO()
        return self.buffer

    def __exit__(self, *args) -> None:
        with s3_client.S3Client(self.url, **self.s3_kwargs) as client:
            # Reposition to start of the stream
            self.buffer.seek(0)
            client.put(  # type: ignore
                self.buffer, bucket_name=self.bucket_name, key_name=self.key_name
            )
        self.buffer = io.BytesIO()
