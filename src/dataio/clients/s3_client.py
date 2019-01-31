import io
from typing import Optional, Tuple, Union, cast

import boto3
from botocore import client as boto_client
from dataio import protocols, urls

from . import decorators, exceptions, stream_client

__all__ = ["S3Client"]


class S3Client(stream_client.StreamClient):
    """
    Generic S3 hook, backed by boto3

    Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
    """

    conn: Optional[boto_client.BaseClient]
    aws_profile: Optional[str]
    conn_encrypt: bool
    aws_access_key_id: Optional[str]
    aws_secret_access_key: Optional[str]
    bucket: Optional[str]
    key_name: Optional[str]

    def __init__(
        self, url: Union[urls.URL, str], aws_profile: str = None, conn_encrypt: bool = False
    ) -> None:
        self.aws_profile = aws_profile
        self.conn_encrypt = conn_encrypt
        super().__init__(url)

        if self.url.scheme != "s3":
            raise exceptions.S3Error(f"Incorrect scheme {self.url.scheme}")

        self.aws_access_key_id = self.url.username or None
        self.aws_secret_access_key = self.url.password or None
        self.bucket = self.url.hostname or None
        self.key_name = self.url.path[1:] if self.url.path != "" else ""

        if self.bucket == "s3":
            self.bucket = None

    # Connection methods:

    def connect(self) -> boto_client.BaseClient:
        # Revert to Boto default credentials

        # Exception: 's3' hostname not a valid bucket

        session = boto3.session.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            profile_name=self.aws_profile,
        )
        return session.client("s3")

    def close(self) -> None:
        # s3 doesn't have close method
        if self.conn is not None:
            self.conn = None

    # Stream methods:

    @decorators.check_conn()
    def get(self, bucket_name: str = None, key_name: str = None) -> protocols.ReaderClosable:
        s3_bucket, s3_key = self._fetch_bucket_and_key(bucket_name, key_name)

        if not self._isfile(s3_bucket, s3_key):
            raise exceptions.S3Error("Unable to fetch the remote file")

        f = io.BytesIO()
        self.conn.download_fileobj(s3_bucket, s3_key, f)  # type: ignore
        f.seek(0)
        return f

    @decorators.check_conn()
    def put(
        self, file_obj: protocols.Reader, bucket_name: str = None, key_name: str = None
    ) -> None:
        s3_bucket, s3_key = self._fetch_bucket_and_key(bucket_name, key_name)

        extra_args = {}
        if self.conn_encrypt:
            extra_args["ServerSideEncryption"] = "AES256"

        cast(boto_client.BaseClient, self.conn).upload_fileobj(
            file_obj, s3_bucket, s3_key, ExtraArgs=extra_args
        )

    # Helpers:

    def _fetch_bucket_and_key(self, bucket: Optional[str], key: Optional[str]) -> Tuple[str, str]:
        bucket_name = bucket or self.bucket
        if bucket_name is None:
            raise exceptions.S3Error("Missing remote bucket")

        key_name = key or self.key_name
        if key_name == "":
            raise exceptions.S3Error("Missing remote key")

        return bucket_name, cast(str, key_name)

    def _isfile(self, bucket: str, key: str) -> bool:
        """
        Checks if a key exists in a bucket
        """
        try:
            self.conn.head_object(Bucket=bucket, Key=key)  # type: ignore
            return True
        except Exception:
            return False
