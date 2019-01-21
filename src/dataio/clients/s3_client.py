import io
from typing import Optional

import boto3
from botocore import client as boto_client

from . import base_client, decorators, exceptions, types


__all__ = ["S3Client"]


class S3Client(base_client.BaseClient, base_client.ReadableMixin):
    """
    Generic S3 hook, backed by boto3
    """

    conn: Optional[boto_client.BaseClient]
    aws_profile: Optional[str]
    conn_encrypt: bool

    def __init__(
        self, url: types.NoneString, aws_profile: str = None, conn_encrypt: bool = False
    ) -> None:
        self.aws_profile = aws_profile
        self.conn_encrypt = conn_encrypt
        super().__init__(url)

        if self.url.scheme != "s3":
            raise exceptions.S3Error(f"Incorrect scheme {self.url.scheme}")

        # Revert to Boto default credentials
        if self.url.username == "":
            self.url.username = None
        if self.url.password == "":
            self.url.password = None

        # Exception: 's3' hostname not a valid bucket
        if self.url.hostname == "s3":
            self.url.hostname = None

        # Exception: key not a path
        if self.url.path != "":
            self.url.path = self.url.path[1:]

    # Connection methods:

    def get_conn(self) -> boto_client.BaseClient:
        session = boto3.session.Session(
            aws_access_key_id=self.url.username,
            aws_secret_access_key=self.url.password,
            profile_name=self.aws_profile,
        )
        return session.client("s3")

    # Document methods:

    @decorators.check_conn()
    def get(self, bucket_name: str = None, key_name: str = None) -> io.BytesIO:
        s3_bucket = bucket_name or self.url.hostname
        if s3_bucket is None:
            raise exceptions.S3Error("Missing remote bucket")

        s3_key = key_name or self.url.path
        if s3_key == "":
            raise exceptions.S3Error("Missing remote key")

        if not self._isfile(s3_bucket, s3_key):
            raise exceptions.S3Error("Unable to fetch the remote file")

        f = io.BytesIO()
        self.conn.download_fileobj(s3_bucket, s3_key, f)  # type: ignore
        return f

    @decorators.check_conn()
    def put(self, file_obj: io.BytesIO, bucket_name: str = None, key_name: str = None) -> None:
        s3_bucket = bucket_name or self.url.hostname
        if s3_bucket is None:
            raise exceptions.S3Error("Missing remote bucket")

        s3_key = key_name or self.url.path
        if s3_key == "":
            raise exceptions.S3Error("Missing remote key")

        extra_args = {}
        if self.conn_encrypt:
            extra_args["ServerSideEncryption"] = "AES256"

        self.conn.upload_fileobj(file_obj, s3_bucket, s3_key, ExtraArgs=extra_args)  # type: ignore

    # Helpers:

    def _isfile(self, bucket: str, key: str) -> bool:
        """
        Checks if a key exists in a bucket
        """
        try:
            self.conn.head_object(Bucket=bucket, Key=key)  # type: ignore
            return True
        except Exception:
            return False
