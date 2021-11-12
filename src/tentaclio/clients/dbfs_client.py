"""DBFS filesystem client."""
from pathlib import Path
from typing import Union

from tentaclio import urls
from tentaclio.clients.local_fs_client import LocalFSClient


__all__ = ["DBFSClient"]


# Allow mounted dbfs to be treated as local file
# Creates a DBFS client for Tentaclio
class DBFSClient(LocalFSClient):
    """DBFS filesystem client implementation."""

    PATH_PREFIX = "/dbfs/"
    allowed_schemes = ["dbfs"]

    def __init__(self, url: Union[urls.URL, str]) -> None:
        """Create a new DBFSClient client."""
        super().__init__(url)

        # On Databricks, files are mounted to /dbfs/. File URLS
        # should be prefixed with /dbfs/
        self.path = str(Path(DBFSClient.PATH_PREFIX) / Path(self.url.path).relative_to('/'))
