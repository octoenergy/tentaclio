import json
import tempfile

import pytest

from tentaclio.clients import GoogleDriveFSClient
from tentaclio.clients.google_drive_client import (
    _GoogleFileDescriptor,
    _ListFilesRequest,
    _load_credentials
)


@pytest.fixture
def token_file():
    with tempfile.NamedTemporaryFile() as f:
        with open(f.name, "w") as writer:
            json.dump({"token": "toktok"}, writer)
        yield f.name


def test_load_credentials_bad_file():
    with pytest.raises(ValueError, match="Token file is not valid"):
        _load_credentials("not_a_valid_file")


def test_load_credentials(mocker, token_file):
    mocked_creds = mocker.patch("tentaclio.clients.google_drive_client.Credentials")
    mocked_creds.return_value = mocker.MagicMock()
    mocked_creds.return_value.valid = True
    token = _load_credentials(token_file)
    token.token == "toktok"


def test_load_not_refreshing(mocker, token_file):
    mocked_creds = mocker.patch("tentaclio.clients.google_drive_client.Credentials")
    mocked_creds.return_value = mocker.MagicMock()
    mocked_creds.return_value.valid = False
    mocked_creds.return_value.refresh_token = False
    with pytest.raises(ValueError, match="Couldn't refresh token"):
        _load_credentials(token_file)


def test_load_refreshing(mocker, token_file):
    mocked_creds = mocker.patch("tentaclio.clients.google_drive_client.Credentials")
    mocked_creds.return_value = mocker.MagicMock()
    mocked_creds.return_value.valid = False
    mocked_creds.return_value.refresh_token = "refresh"
    mocked_creds.return_value.to_json.return_value = '{"token": "refreshed"}'
    _load_credentials(token_file)
    with open(token_file) as f:
        creds = json.load(f)
    assert creds == {"token": "refreshed"}


class TestGoogleDriveFSClient:
    @pytest.mark.parametrize(
        ("url, drive, path_parts"),
        (
            ("gdrive://My Drive/", "root", ()),
            ("gdrive://My Drive/path/to/dir/", "root", ("path", "to", "dir")),
            ("gdrive://My Drive/path/to/dir/file.txt", "root", ("path", "to", "dir", "file.txt")),
        ),
    )
    def test_parse_path(self, url, drive, path_parts):
        client = GoogleDriveFSClient(url)
        assert client.drive == drive
        assert client.path_parts == path_parts

    def test_parse_path_empty(self):
        with pytest.raises(ValueError):
            GoogleDriveFSClient("googledrive://")


class TestGoogleFileDescriptor:
    def test_is_dir(self):
        args = dict(
            name="file",
            id_="123",
            mime_type=_GoogleFileDescriptor.FOLDER_MIME_TYPE,
            parents=[],
            url="googledrive://root/",
        )
        descriptor = _GoogleFileDescriptor(**args)
        assert descriptor.is_dir
        assert not descriptor.is_file

    def test_is_not_dir(self):
        args = dict(
            name="file",
            id_="123",
            mime_type="application/other",
            parents=[],
            url="googledrive://root/",
        )
        descriptor = _GoogleFileDescriptor(**args)
        assert not descriptor.is_dir
        assert descriptor.is_file


class TestListFilesRequest:
    @pytest.fixture
    def file_props(self):
        return {
            "id": "123",
            "name": "file",
            "parents": ["0"],
            "mimeType": "application/thingy",
        }

    def test_build_descriptor(self, mocker, file_props):
        lister = _ListFilesRequest(mocker.Mock)
        descriptor = next(lister._build_descriptors([file_props], None))
        assert descriptor.id_ == file_props["id"]
        assert descriptor.name == file_props["name"]
        assert descriptor.parents == file_props["parents"]
        assert descriptor.mime_type == file_props["mimeType"]

    def test_build_descriptor_with_url(self, mocker, file_props):
        lister = _ListFilesRequest(mocker.Mock)
        descriptor = next(lister._build_descriptors([file_props], "googledrive://my drive"))
        assert descriptor.id_ == file_props["id"]
        assert descriptor.name == file_props["name"]
        assert descriptor.parents == file_props["parents"]
        assert descriptor.mime_type == file_props["mimeType"]
        assert str(descriptor.url) == "googledrive://my drive/file"

    def test_list_no_pagination(self, mocker, file_props):
        service = mocker.MagicMock()
        response = {
            "files": [file_props],
        }
        service.files.return_value.list.return_value.execute.return_value = response
        lister = _ListFilesRequest(service)
        results = list(lister.list())
        assert len(results) == 1
        assert results[0].id_ == file_props["id"]

    def test_list_pagination(self, mocker, file_props):
        service = mocker.MagicMock()
        file_props_2 = file_props.copy()
        file_props_2["id"] = "124"
        responses = [
            {"files": [file_props], "nextPageToken": "please"},
            {"files": [file_props_2]},
        ]

        service.files.return_value.list.return_value.execute.side_effect = responses
        lister = _ListFilesRequest(service)
        results = list(lister.list())
        assert len(results) == 2
        assert results[0].id_ == file_props["id"]
        assert results[1].id_ == file_props_2["id"]
