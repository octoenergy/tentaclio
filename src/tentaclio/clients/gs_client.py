"""GS Stream client."""
from typing import Optional, Union

from google.cloud import storage

from tentaclio import urls

from . import base_client


__all__ = ["GSClient"]


class GSClient(base_client.BaseClient["GSClient"]):
    """GS client.

    Ref: https://googleapis.dev/python/storage/latest/index.html
    """

    allowed_schemes = ["gs"]

    conn: storage.Client
    bucket: Optional[str]
    key_name: Optional[str]

    def __init__(
        self, url: Union[urls.URL, str]
    ) -> None:
        """Create a new GS client.

        The client is created based on a URL of the form
        gs://bucket/key.
        Authenticated with environment variables:
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
