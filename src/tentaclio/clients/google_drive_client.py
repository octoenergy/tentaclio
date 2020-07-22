"""Local filesystem client."""
from typing import Iterable, Tuple, Union

from tentaclio import fs, protocols, urls

from . import base_client


__all__ = ["GoogleDriveFSClient"]


class GoogleDriveFSClient(base_client.BaseClient["GoogleDriveFSClient"]):
    """Allow filesystem-like access to google drive."""

    DEFAULT_DRIVE = "root"

    allowed_schemes = ["gdrive", "googledrive"]

    drive: str
    parts: Tuple[str]

    def __init__(self, url: Union[urls.URL, str]) -> None:
        """Create a new GoogleDriveFSClient."""
        super().__init__(url)

        if len(self.url.path) == 0:
            raise ValueError(
                f"Bad url: {self.url.path} .Google Drive needs at least "
                "the drive part (i.e. gdrive://My Drive)"
            )
        parts = self.url.path.split("/")
        # FIXME for the moment the drive is pointing to the magic root
        # drive (My Drive)
        self.drive = self.DEFAULT_DRIVE
        self.path_parts = tuple(filter(lambda part: len(part) > 0, parts[1:]))

    def _connect(self) -> "GoogleDriveFSClient":
        return self

    def close(self) -> None:
        """Close the dummy connection to google drive."""
        self.closed = True

    # Stream methods:

    def get(self, writer: protocols.ByteWriter, **kwargs) -> None:
        """Get the contents of the google drive file."""
        pass

    def put(self, reader: protocols.ByteReader, **kwargs) -> None:
        """Write the contents of the reader to the google drive file."""
        pass

    # scandir related methods

    def scandir(self, **kwargs) -> Iterable[fs.DirEntry]:
        """List contents of a folder from google drive."""
        return []

    # remove

    def remove(self):
        """Remove the file from google drive."""
        pass
