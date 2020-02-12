"""S3 Stream client."""
from typing import Generator, Iterable, Optional, Tuple, Union, cast

import boto3
from botocore import client as boto_client

from tentaclio import fs, protocols, urls

from . import base_client, decorators, exceptions


__all__ = ["S3Client"]


class S3Client(base_client.BaseClient["S3Client"]):
    """S3 client, backed by boto3.

    Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
    """

    allowed_schemes = ["s3"]

    conn: boto_client.BaseClient
    aws_profile: Optional[str]
    conn_encrypt: bool
    aws_access_key_id: Optional[str]
    aws_secret_access_key: Optional[str]
    bucket: Optional[str]
    key_name: Optional[str]

    def __init__(
        self, url: Union[urls.URL, str], aws_profile: str = None, conn_encrypt: bool = False
    ) -> None:
        """Create a new S3 client.

        The client is created based based on a url of the form
        s3://access_key_id:secret_access_key@bucket/key.
        If the access key and the secret are not provided, the authentication
        will be delegated to boto.
        """
        self.aws_profile = aws_profile
        self.conn_encrypt = conn_encrypt
        super().__init__(url)

        self.aws_access_key_id = self.url.username or None
        self.aws_secret_access_key = self.url.password or None
        self.bucket = self.url.hostname or None
        self.key_name = self.url.path[1:] if self.url.path != "" else ""

        if self.bucket == "s3":
            self.bucket = None

    def _connect(self) -> boto_client.BaseClient:
        # Revert to Boto default credentials

        # Exception: 's3' hostname not a valid bucket

        session = boto3.session.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            profile_name=self.aws_profile,
        )
        return session.client("s3")

    def close(self) -> None:
        """Close the connection.

        This is fake. Boto doesn't allow to close s3 connections.
        """
        # s3 doesn't have close method
        if self.closed:
            raise ValueError("Trying to close a closed client")
        self.closed = True

    # Stream methods:

    @decorators.check_conn
    def get(
        self, writer: protocols.ByteWriter, bucket_name: str = None, key_name: str = None
    ) -> None:
        """Download the contents from s3 and write them in the provided writer.

        Arguments:
            :bucket_name: If not provided in the url at construction time.
            :key_name: If not provided in the url at construction time.
        """
        s3_bucket, s3_key = self._fetch_bucket_and_key(bucket_name, key_name)

        if not self._isfile(s3_bucket, s3_key):
            raise exceptions.S3Error("Unable to fetch the remote file")

        self.conn.download_fileobj(s3_bucket, s3_key, writer)

    @decorators.check_conn
    def put(
        self, reader: protocols.ByteReader, bucket_name: str = None, key_name: str = None
    ) -> None:
        """Up the contents of the reader to s3.

        Arguments:
            :bucket_name: If not provided in the url at construction time.
            :key_name: If not provided in the url at construction time.
        """
        s3_bucket, s3_key = self._fetch_bucket_and_key(bucket_name, key_name)

        extra_args = {}
        if self.conn_encrypt:
            extra_args["ServerSideEncryption"] = "AES256"

        self.conn.upload_fileobj(reader, s3_bucket, s3_key, ExtraArgs=extra_args)

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
        """Check if a key exists in a bucket."""
        try:
            self.conn.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            return False

    # scandir related methods

    def scandir(self, **kwargs) -> Iterable[fs.DirEntry]:
        """Scan the connection url to create dir entries."""
        if self._is_root():
            return self._build_bucket_entries()
        return _KeyLister(self)

    def _build_bucket_entries(self) -> Iterable[fs.DirEntry]:
        return (
            fs.DirEntry(url=urls.URL("s3://" + name), is_dir=True, is_file=False)
            for name in self._get_buckets()
        )

    def _get_buckets(self):
        resp = self.conn.list_buckets()
        buckets = []
        for metadata in resp["Buckets"]:
            buckets.append(metadata["Name"])
        return buckets

    def _is_root(self):
        return not self.bucket

    # Copy methods

    def copy(self, source: urls.URL, dest: urls.URL):
        """Copy source into dest directly in S3."""
        source_client = S3Client(source)
        dest_client = S3Client(dest)
        with source_client:
            source_client.conn.copy(
                CopySource={"Bucket": source_client.bucket, "Key": source_client.key_name},
                Bucket=dest_client.bucket,
                Key=dest_client.key_name,
            )

    @decorators.check_conn
    def remove(self):
        """Remove the key from the aws bucket."""
        self.conn.delete_object(Bucket=self.bucket, Key=self.key_name)


class _KeyLister(Iterable[fs.DirEntry]):
    """List S3 keys.

    Heavily inspired by
    https://github.com/dask/s3fs/blob/05fc3d9c64a5f9ff701f9d9541f707aff5750349/s3fs/core.py#L357
    """

    def __init__(self, client: S3Client):
        self.paginator = client.conn.get_paginator("list_objects_v2")
        self.bucket = client.bucket
        self.key = client.key_name

    def _get_delimeted_key(self):
        if self.key and self.key[-1] != "/":
            return self.key + "/"
        return self.key

    def __iter__(self) -> Generator[fs.DirEntry, None, None]:
        for page in self.paginator.paginate(
            Bucket=self.bucket, Prefix=self._get_delimeted_key(), Delimiter="/"
        ):
            yield from self._iter_page(page)

    def _iter_page(self, page) -> Generator[fs.DirEntry, None, None]:
        dirs = [entry["Prefix"] for entry in page.get("CommonPrefixes", [])]
        files = [entry["Key"] for entry in page.get("Contents", [])]
        for dir_ in dirs:
            yield fs.build_folder_entry(_build_url(str(self.bucket), dir_))

        for file_ in files:
            yield fs.build_file_entry(_build_url(str(self.bucket), file_))


def _build_url(bucket: str, prefix: str) -> urls.URL:
    if prefix:
        prefix = prefix.rstrip("/")

    return urls.URL(f"s3://{bucket}/{prefix}")
