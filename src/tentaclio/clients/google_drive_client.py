"""Google drive client."""
import abc
import functools
import json
import logging
import mimetypes
import os
from dataclasses import dataclass
from typing import Any, Dict, Generic, Iterable, List, Optional, Tuple, TypeVar, Union

from apiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from tentaclio import fs, protocols, urls

from . import base_client, decorators


logger = logging.getLogger(__name__)

__all__ = ["GoogleDriveFSClient"]

DEFAULT_TOKEN_FILE = os.environ["HOME"] + os.sep + ".tentaclio_google_drive.json"
# Load the location of the token file from the environment
TOKEN_FILE = os.getenv("TENTACLIO__GOOGLE_DRIVE_TOKEN_FILE", DEFAULT_TOKEN_FILE)

# Generic type
T = TypeVar("T")


# Credentials management


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


# Google drive object descriptors
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


@dataclass
class _GoogleDriveDescriptor:
    id_: str
    name: str
    root_descriptor: _GoogleFileDescriptor


class GoogleDriveFSClient(base_client.BaseClient["GoogleDriveFSClient"]):
    """Allow filesystem-like access to google drive.

    Google drive follows a drive oriented architecture more reminiscent of windows filesystems
    than unix approaches. This makes a bit complicated to present the resources as a URLs.

    From the user perspective accessing the resources works as the following
    * urls MUST have an empty hostname `gdrive:///My Drive/` or `gdrive:/My Drive/`

    * the first element of the path has to be the drive name i.e. `My Drive` for the default
    drive or the drive name as it appears in the web ui for shared drives.
    """

    DEFAULT_DRIVE_NAME = "My Drive"
    DEFAULT_DRIVE_ID = "root"
    DEFAULT_DRIVE_DESCRIPTOR = _GoogleDriveDescriptor(
        id_=DEFAULT_DRIVE_ID,
        name=DEFAULT_DRIVE_NAME,
        root_descriptor=_GoogleFileDescriptor(
            id_=DEFAULT_DRIVE_ID,
            name=DEFAULT_DRIVE_NAME,
            mime_type=_GoogleFileDescriptor.FOLDER_MIME_TYPE,
            url=urls.URL("gdrive:///{self.DEFAULT_DRIVE}"),
            parents=[],
        ),
    )

    allowed_schemes = ["gdrive", "googledrive"]

    drive_name: str
    path_parts: Tuple[str, ...]

    # Not an easy task to figure out the type of the
    # returned value from the library
    _service: Optional[Any] = None

    def __init__(self, url: Union[urls.URL, str]) -> None:
        """Create a new GoogleDriveFSClient."""
        super().__init__(url)

        parts = list(filter(lambda part: len(part) > 0, self.url.path.split("/")))
        if len(parts) == 0:
            raise ValueError(
                f"Bad url: {self.url.path} :Google Drive needs at least "
                "the drive part (i.e. gdrive:///My Drive/)"
            )
        self.drive_name = parts[0]
        self.path_parts = tuple(parts[1:])

    @property
    def _drive(self):
        drives = self._get_drives()
        if self.drive_name not in drives:
            names = [d for d in drives]
            raise ValueError(f"Drive name (hostname) should be one of {names}")
        return drives[self.drive_name]

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
        leaf_descriptor = self._get_leaf_descriptor()
        _DownloadRequest(self._service, leaf_descriptor.id_, writer).execute()

    def put(self, reader: protocols.ByteReader, **kwargs) -> None:
        """Write the contents of the reader to the google drive file."""
        try:
            file_descriptor = self._get_leaf_descriptor()
            self._update(file_descriptor, reader)
        except IOError:
            # file doesn't exist, then create
            self._create(reader)

    def _create(self, reader: protocols.ByteReader):
        """Create a new file."""
        descriptors = self._get_path_descriptors(ignore_tail=True)

        parent = descriptors[-1]
        if not parent.is_dir:
            raise IOError(f"{self.url} parent path is not a folder")

        uploader = _CreateRequest(
            service=self._service, name=self.path_parts[-1], parent_id=parent.id_, reader=reader
        )
        uploader.execute()

    def _update(self, file_descriptor: _GoogleFileDescriptor, reader: protocols.ByteReader):
        """Update file contents."""
        uploader = _UpdateRequest(
            service=self._service,
            name=file_descriptor.name,
            file_id=file_descriptor.id_,
            reader=reader,
        )
        uploader.execute()

    # scandir related methods

    @decorators.check_conn
    def scandir(self, **kwargs) -> Iterable[fs.DirEntry]:
        """List contents of a folder from google drive."""
        leaf_descriptor = self._get_leaf_descriptor()

        if not leaf_descriptor.is_dir:
            raise IOError(f"{self.url} is not a folder")

        url_base = str(self.url).rstrip("/") + "/"
        lister = _ListFilesRequest(
            self._service, url_base=url_base, q=f"'{leaf_descriptor.id_}' in parents"
        )
        return lister.list()

    # remove

    def remove(self):
        """Remove the file from google drive."""
        leaf_descriptor = self._get_leaf_descriptor()
        args = {
            "fileId": leaf_descriptor.id_,
            "supportsTeamDrives": True,
        }
        self._service.files().delete(**args).execute()

    @functools.lru_cache(maxsize=1)
    def _get_drives(self) -> Dict[str, _GoogleDriveDescriptor]:
        drives = {d.name: d for d in _ListDrivesRequest(self._service).list()}
        drives[self.DEFAULT_DRIVE_NAME] = self.DEFAULT_DRIVE_DESCRIPTOR
        return drives

    def _get_leaf_descriptor(self) -> _GoogleFileDescriptor:
        """Get the last descriptor from the path part of the url."""
        return self._get_path_descriptors()[-1]

    def _get_path_descriptors(self, ignore_tail=False) -> List[_GoogleFileDescriptor]:
        parts = self.path_parts
        if ignore_tail:
            parts = parts[:-1]
        return list(self._path_parts_to_descriptors(self._drive, parts))

    def _path_parts_to_descriptors(
        self, drive: _GoogleDriveDescriptor, path_parts: Iterable[str]
    ) -> List[_GoogleFileDescriptor]:
        """Convert the path parts into google drive descriptors."""
        file_descriptors = [drive.root_descriptor]
        parent = None
        for pathPart in path_parts:
            file_descriptor = self._get_file_descriptor_by_name(pathPart, parent)
            parent = file_descriptor.id_
            file_descriptors.append(file_descriptor)

        return file_descriptors

    def _get_file_descriptor_by_name(self, name: str, parent: Optional[str] = None):
        """Get the file id given the file name and it's parent."""
        args = {"q": f" name = '{name}'"}

        if parent is not None:
            args["q"] += f" and '{parent}' in parents"

        results = list(_ListFilesRequest(self._service, **args).list())

        if len(results) == 0:
            raise IOError(
                f"Descriptor not found for {self.url} "
                f"Could not find part {name} with parent id {parent}"
            )
        return results[0]


class _GoogleDriveRequest:
    """Abstract requests to google drive."""

    args: Dict[str, Any] = {}
    service: Any

    def __init__(self, service: Any, args: Dict[str, Any]):
        self.args = args
        self.service = service


class _DownloadRequest(_GoogleDriveRequest):
    """Download data from google drive."""

    def __init__(self, service: Any, file_id: str, writer: protocols.ByteWriter, **kwargs):
        super().__init__(service, kwargs)
        self.args["fileId"] = file_id
        self.args["supportsTeamDrives"] = True
        self.writer = writer

    def execute(self):
        request = self.service.files().get_media(**self.args)
        downloader = MediaIoBaseDownload(self.writer, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()


class _CreateRequest(_GoogleDriveRequest):
    """Upload data to google drive."""

    def __init__(
        self, service: Any, name: str, parent_id: str, reader: protocols.ByteReader, **kwargs
    ):
        super().__init__(service, kwargs)
        # self.args["parents"] = [parent_id]
        self.args["supportsTeamDrives"] = True
        self.args["name"] = name
        self.args["parents"] = [parent_id]
        self.reader = reader

    def execute(self):
        media_body = MediaIoBaseUpload(
            self.reader, mimetype=mimetypes.guess_type(self.args["name"])[0]
        )
        self.service.files().create(body=self.args, media_body=media_body,).execute()


class _UpdateRequest(_GoogleDriveRequest):
    """Update data to google drive."""

    def __init__(
        self, service: Any, file_id: str, name: str, reader: protocols.ByteReader, **kwargs
    ):
        super().__init__(service, kwargs)
        # self.args["parents"] = [parent_id]
        self.args["supportsTeamDrives"] = True
        self.args["fileId"] = file_id
        self.name = name
        self.reader = reader

    def execute(self):
        media_body = MediaIoBaseUpload(self.reader, mimetype=mimetypes.guess_type(self.name)[0])
        self.service.files().update(media_body=media_body, **self.args).execute()


class _Lister(
    _GoogleDriveRequest, abc.ABC, Generic[T],
):
    def __init__(self, service: Any, **kwargs):
        super().__init__(service, kwargs)
        # some standard arguments to send to the service
        self.args["pageSize"] = 100

    def list(self) -> Iterable[T]:
        """List the resources controlling the pagination."""
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


# Getting the drive root:
# This is quite a hack as there is no direct way, documented or that I could
# find, to get the root folder from a shared drive.
# So the process consists on getting one random file from a drive
# and navigate through the parents until hitting a file descriptor that has no parents.


def _get_drive_root(service: Any, drive_id: str):
    """Get the drive root by navigating up the tree from a random file."""
    done = False
    parent = _get_random_parent(service, drive_id)
    file_args = {"fields": "id, name, mimeType, parents", "supportsAllDrives": True}
    while not done:
        file_args["fileId"] = parent
        result = service.files().get(**file_args).execute()
        if result is None:
            raise IOError("Parent not found while resolving drive root for drive_id: {drive_id}")
        if "parents" not in result:
            args = {
                "id_": result.get("id"),
                "name": result.get("name"),
                "mime_type": result.get("mimeType"),
                "parents": result.get("parents"),
                "url": None,
            }
            return _GoogleFileDescriptor(**args)
        parent = result["parents"][0]


def _get_random_parent(service: Any, drive_id: str):
    """Get random parent from this drive."""
    args: Dict[str, Any] = {
        "driveId": drive_id,
        "corpora": "drive",
        "pageSize": "1",
        "includeItemsFromAllDrives": True,
    }
    lister = _ListFilesRequest(service, **args)
    children = list(lister.list())

    if len(children) == 0:
        raise IOError("No files found while inspecting drive")

    # No parents we're in the root
    if children[0].parents is None or len(children[0].parents) == 0:
        return children[0].id_

    return children[0].parents[0]


# END OF HACK


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
                "root_descriptor": _get_drive_root(self.service, drive.get("id"),),
            }
            yield _GoogleDriveDescriptor(**args)
