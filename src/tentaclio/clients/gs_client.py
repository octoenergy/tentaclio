"""GS Stream client."""
from typing import Optional, Union, Tuple, cast

from google.cloud import storage

from tentaclio import urls, protocols

from . import base_client, decorators, exceptions


__all__ = ["GSClient"]


class GSClient(base_client.BaseClient["GSClient"]):
    """GS client.

    Ref: https://googleapis.dev/python/storage/latest/index.html
    """

    allowed_schemes = ["gs", "gcs"]

    conn: storage.Client
    bucket: Optional[str]
    key_name: Optional[str]

    def __init__(
        self, url: Union[urls.URL, str]
    ) -> None:
        """Create a new GS client.

        The client is created based on a URL of the form
        gs://bucket/key.

        Authentication and client configuration is recommended with environment variables.
        Quoted from docs:
            "Setting the GOOGLE_APPLICATION_CREDENTIALS and GOOGLE_CLOUD_PROJECT environment
            variables will override the automatically configured credentials."

        See documentation for more information.
        https://googleapis.dev/python/google-api-core/latest/auth.html
        """
        super().__init__(url)

        self.bucket = self.url.hostname or None
        self.key_name = self.url.path[1:] if self.url.path != "" else ""

        if self.bucket == "gs":
            self.bucket = None

    def _connect(self) -> storage.Client:
        return storage.Client()

    def close(self) -> None:
        """Close the connection.

        This is fake. GS doesn't allow to close gs connections.
        """
        # gs doesn't have close method
        if self.closed:
            raise ValueError("Trying to close a closed client")
        self.closed = True

    # Stream methods:

    @decorators.check_conn
    def get(
        self,
        writer: protocols.ByteWriter,
        bucket_name: Optional[str] = None,
        key_name: Optional[str] = None
    ) -> None:
        """Download the contents from gs and write them in the provided writer.

        Arguments:
            :bucket_name: If not provided in the url at construction time.
            :key_name: If not provided in the url at construction time.

        Raises:
            GSError: If a bucket or a key is not found in the given URL or args then
            Google Cloud Exceptions: if the client raises them.

        """
        gs_bucket, gs_key = self._fetch_bucket_and_key(bucket_name, key_name)

        self._get(writer, gs_bucket, gs_key)

    @decorators.check_conn
    def put(
        self,
        reader: protocols.ByteReader,
        bucket_name: Optional[str] = None,
        key_name: Optional[str] = None
    ) -> None:
        """Up the contents to gs.

        Arguments:
            :bucket_name: If not provided in the url at construction time.
            :key_name: If not provided in the url at construction time.

        Raises:
            GSError: If a bucket or a key is not found in the given URL or args then
            Google Cloud Exceptions: if the client raises them.

        """
        gs_bucket, gs_key = self._fetch_bucket_and_key(bucket_name, key_name)

        self._put(reader, gs_bucket, gs_key)

    # Non-streaming public methods
    @decorators.check_conn
    def remove(self, bucket_name: Optional[str] = None, key_name: Optional[str] = None) -> None:
        """Remove the object from gs bucket.

        Arguments:
            :bucket_name: If not provided in the url at construction time.
            :key_name: If not provided in the url at construction time.

        Raises:
            GSError: If a bucket or a key is not found in the given URL or args then
            Google Cloud Exceptions: if the client raises them.

        """
        gs_bucket, gs_key_name = self._fetch_bucket_and_key(bucket_name, key_name)
        self._remove(gs_bucket, gs_key_name)

    # Helpers:

    def _fetch_bucket_and_key(
        self, bucket: Optional[str] = None, key: Optional[str] = None
    ) -> Tuple[str, str]:
        bucket_name = bucket or self.bucket
        if bucket_name is None:
            raise exceptions.GSError("Missing remote bucket")

        key_name = key or self.key_name
        if key_name == "":
            raise exceptions.GSError("Missing remote key")

        return bucket_name, cast(str, key_name)

    def _get_blob(self, bucket_name: str, key_name: str) -> storage.Bucket:
        """Get the bucket client."""
        bucket = self.conn.bucket(bucket_name)
        return bucket.blob(key_name)

    def _get(
        self, writer: protocols.ByteWriter, bucket_name: str, key_name: str
    ) -> None:
        """Download file on the client."""
        blob = self._get_blob(bucket_name, key_name)
        blob.download_to_file(writer)

    def _put(
        self, reader: protocols.ByteReader, bucket_name: str, key_name: str
    ) -> None:
        """Upload on the client."""
        blob = self._get_blob(bucket_name, key_name)
        blob.upload_from_file(reader)

    def _remove(
        self, bucket_name: str, key_name: str
    ) -> None:
        """Delete on the client."""
        blob = self._get_blob(bucket_name, key_name)
        blob.delete()
