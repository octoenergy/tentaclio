"""Local filesystem client."""
import json
import os
from typing import Any, Iterable, Optional, Tuple, Union

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from tentaclio import fs, protocols, urls

from . import base_client


__all__ = ["GoogleDriveFSClient"]

# Load the location of the token file from the environment
TOKEN_FILE = os.getenv(
    "TENTACLIO__GOOGLE_DRIVE_TOKEN_FILE", os.environ["HOME"] + os.sep + ".google_drive_token.json",
)


def _load_credentials(token_file: str) -> Credentials:
    """Load the credentials and refresh them if necesary."""
    creds = None
    if os.path.exists(token_file):
        with open(token_file) as f:
            state = json.load(f)
            creds = Credentials(**state)
    else:
        raise ValueError(f"Token file is not valid {token_file}")

    # If there are no (valid) credentials available refresh them or raise an error.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            raise ValueError(f"Couldn't refresh token in f{token_file}")
        # Save the credentials for the next run
        with open(token_file, "w") as f:
            f.write(creds.to_json())
    return creds


class GoogleDriveFSClient(base_client.BaseClient["GoogleDriveFSClient"]):
    """Allow filesystem-like access to google drive."""

    DEFAULT_DRIVE = "root"

    allowed_schemes = ["gdrive", "googledrive"]

    drive: str
    parts: Tuple[str]

    # Not an easy task to figure out the type of the
    # returned value from the library
    _service: Optional[Any] = None

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
        self._refresh_service()
        return self

    def _refresh_service(self, token_file: str = TOKEN_FILE):
        """Check the validity of the credentials."""
        if self._service is not None:
            return
        creds = _load_credentials(token_file)
        self._service = build("drive", "v3", credentials=creds)

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
