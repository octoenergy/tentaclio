"""Local filesystem client."""
import abc
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Generic, Iterable, List, Optional, Tuple, TypeVar, Union

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from tentaclio import fs, protocols, urls

from . import base_client, decorators


__all__ = ["GoogleDriveFSClient"]

# Load the location of the token file from the environment
TOKEN_FILE = os.getenv(
    "TENTACLIO__GOOGLE_DRIVE_TOKEN_FILE", os.environ["HOME"] + os.sep + ".google_drive_token.json",
)

# Generic type
T = TypeVar("T")


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

    DEFAULT_DRIVE = "My Drive"

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

    @decorators.check_conn
    def scandir(self, **kwargs) -> Iterable[fs.DirEntry]:
        """List contents of a folder from google drive."""
        # get the descriptor for the leaf folder
        leaf_descriptor = list(
            _path_parts_to_descriptors(self._service, self.drive, self.path_parts)
        )[-1]

        if not leaf_descriptor.is_dir:
            raise IOError(f"{self.url} is not a folder")

        url_base = str(self.url).rstrip("/") + "/"
        lister = _ListFilesRequest(
            self._service, url_base=url_base, q=f"'{leaf_descriptor.id_}' in parents"
        )
        return lister.list()

    def _get_drives(self) -> Iterable["_GoogleDriveDescriptor"]:
        return _ListDrivesRequest(self._service).list()

    # remove

    def remove(self):
        """Remove the file from google drive."""
        pass


def _path_parts_to_descriptors(service: Any, drive: str, path_parts: Iterable[str]):
    """Convert the path parts into google drive ids."""
    # FIXME better driver support
    file_descriptors = [
        _GoogleFileDescriptor(
            id_=drive,
            name="root",
            mime_type=_GoogleFileDescriptor.FOLDER_MIME_TYPE,
            url=urls.URL("gdrive://root"),
            parents=[],
        )
    ]
    parent = None
    for pathPart in path_parts:
        file_descriptor = _get_file_descriptor(service, pathPart, parent)
        parent = file_descriptor.id_
        file_descriptors.append(file_descriptor)

    return file_descriptors


def _get_file_descriptor(service: Any, name: str, parent: Optional[str] = None):
    """Get the file id given the file name and it's parent."""
    args = {"q": f" name = '{name}'"}
    if parent is not None:
        args["q"] += f" and '{parent}' in parents"

    results = list(_ListFilesRequest(service, **args).list())

    if len(results) == 0:
        raise RuntimeError(f"Could not find file {name} with parent {parent}")
    return results[0]


class _GoogleDriveRequest:
    """Abstract requests to google drive."""

    args: Dict[str, Any] = {}
    service: Any

    def __init__(self, service: Any, args: Dict[str, Any]):
        self.args = args
        self.service = service


@dataclass
class _GoogleFileDescriptor(fs.DirEntry):
    FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"

    id_: str
    name: str
    mime_type: str
    parents: List[str]
    url: urls.URL

    @property
    def is_dir(self):
        return self.mime_type == self.FOLDER_MIME_TYPE

    @property
    def is_file(self):
        return not self.is_dir


class _Lister(
    _GoogleDriveRequest, abc.ABC, Generic[T],
):
    def __init__(self, service: Any, **kwargs):
        super().__init__(service, kwargs)
        # some standard arguments to send to the service
        self.args["pageSize"] = 100

    def list(self) -> Iterable[T]:
        done = False
        while not done:
            results = self._execute()

            yield from self._yielder(results)
            # check if we need to keep on getting pages
            self.args["pageToken"] = results.get("nextPageToken", None)
            done = self.args["pageToken"] is None

    @abc.abstractmethod
    def _execute(self) -> Any:
        pass

    @abc.abstractmethod
    def _yielder(self, results) -> Iterable[T]:
        pass


class _ListFilesRequest(_Lister[_GoogleFileDescriptor]):
    def __init__(self, service: Any, url_base: Optional[str] = None, **kwargs):
        super().__init__(service, **kwargs)
        # Get team drives too
        self.args["supportsTeamDrives"] = True
        self.args["includeTeamDriveItems"] = True
        self.args["fields"] = "files(id, name, mimeType, parents)"
        self.url_base = url_base

    def _execute(self) -> Any:
        return self.service.files().list(**self.args).execute()

    def _yielder(self, results) -> Iterable[_GoogleFileDescriptor]:
        yield from self._build_descriptors(results.get("files", []))

    def _build_descriptors(self, files: List[Any]) -> Iterable[_GoogleFileDescriptor]:
        for f in files:
            args = {
                "id_": f.get("id"),
                "name": f.get("name"),
                "mime_type": f.get("mimeType"),
                "parents": f.get("parents"),
                "url": None,
            }
            if self.url_base is not None:
                args["url"] = urls.URL(self.url_base + args["name"])
            yield _GoogleFileDescriptor(**args)


@dataclass
class _GoogleDriveDescriptor:
    id_: str
    name: str


class _ListDrivesRequest(_Lister[_GoogleDriveDescriptor]):
    def __init__(self, service: Any, **kwargs):
        super().__init__(service, **kwargs)
        self.args["fields"] = "drives(id, name)"

    def _execute(self) -> Any:
        return self.service.drives().list(**self.args).execute()

    def _yielder(self, results) -> Iterable[_GoogleDriveDescriptor]:
        for drive in results.get("drives", []):
            args = {
                "id_": drive.get("id"),
                "name": drive.get("name"),
            }
            yield _GoogleDriveDescriptor(**args)
