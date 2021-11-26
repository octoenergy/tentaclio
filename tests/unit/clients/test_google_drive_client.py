import copy
import io
import json
import tempfile

import pytest

from tentaclio import urls
from tentaclio.clients import GoogleDriveFSClient
from tentaclio.clients.google_drive_client import (
    _CreateRequest,
    _get_drive_root,
    _get_random_parent,
    _GoogleFileDescriptor,
    _ListDrivesRequest,
    _ListFilesRequest,
    _load_credentials,
    _UpdateRequest,
    _get_default_token_file
)


@pytest.fixture
def token_file():
    with tempfile.NamedTemporaryFile() as f:
        with open(f.name, "w") as writer:
            json.dump({"token": "toktok"}, writer)
        yield f.name


@pytest.fixture
def token_file_unknown_field():
    with tempfile.NamedTemporaryFile() as f:
        with open(f.name, "w") as writer:
            json.dump({"token": "toktok", "superunknown": "sg"}, writer)
        yield f.name


@pytest.fixture
def file_props():
    return {
        "id": "123",
        "name": "file",
        "parents": ["0"],
        "mimeType": "application/thingy",
    }


@pytest.fixture
def drive_props():
    return {
        "id": "123",
        "name": "drive",
    }


@pytest.fixture
def file_descriptor():
    return _GoogleFileDescriptor(
        id_="123",
        name="file",
        parents=["0"],
        url=urls.URL("gdrive://My Drive/"),
        mime_type="application/thingy",
    )


@pytest.fixture
def folder_descriptor():
    return _GoogleFileDescriptor(
        id_="124",
        name="folder",
        parents=["0"],
        url=urls.URL("gdrive://My Drive/"),
        mime_type=_GoogleFileDescriptor.FOLDER_MIME_TYPE,
    )


@pytest.fixture
def mocked_service(mocker, file_props):
    service = mocker.MagicMock()
    response = {
        "files": [file_props],
    }
    service.files.return_value.list.return_value.execute.return_value = response
    return service


def test_load_credentials_bad_file():
    with pytest.raises(ValueError, match="Token file is not valid"):
        _load_credentials("not_a_valid_file")


def test_load_credentials(mocker, token_file):
    mocked_creds = mocker.patch("tentaclio.clients.google_drive_client.Credentials")
    mocked_creds.return_value = mocker.MagicMock()
    mocked_creds.return_value.valid = True
    token = _load_credentials(token_file)
    token.token == "toktok"


def test_load_credentials_unknown_field(mocker, token_file_unknown_field):
    mocked_creds = mocker.patch("tentaclio.clients.google_drive_client.Credentials")

    def _credentials(x):
        x.vaild = True
        return x

    mocked_creds.__call__ = _credentials
    creds = _load_credentials(token_file_unknown_field)
    assert "superunknown" not in creds
    assert "token" not in creds


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
    @pytest.fixture
    def client(self, mocker, file_descriptor):
        client = GoogleDriveFSClient("gdrive:///My Drive/file")
        client._get_leaf_descriptor = mocker.MagicMock()
        client._get_leaf_descriptor.return_value = file_descriptor
        client._get_path_descriptors = mocker.MagicMock()
        client._service = mocker.MagicMock()
        return client

    @pytest.mark.parametrize(
        ("url, drive, path_parts"),
        (
            ("gdrive:///My Drive/", "My Drive", ()),
            ("gdrive:///My Drive/path/to/dir/", "My Drive", ("path", "to", "dir")),
            (
                "gdrive:///My Drive/path/to/dir/file.txt",
                "My Drive",
                ("path", "to", "dir", "file.txt"),
            ),
        ),
    )
    def test_parse_path(self, url, drive, path_parts):
        client = GoogleDriveFSClient(url)
        assert client.drive_name == drive
        assert client.path_parts == path_parts

    def test_parse_path_empty(self):
        with pytest.raises(ValueError):
            GoogleDriveFSClient("googledrive://")

    def test_scandir_file(self, client):

        with pytest.raises(IOError, match=("not a folder")), client:
            client.scandir()

    @pytest.mark.parametrize("url", ("gdrive:///My Drive/folder/", "gdrive:///My Drive/folder",))
    def test_scandir_folder(self, mocker, folder_descriptor, url):
        lister = mocker.patch("tentaclio.clients.google_drive_client._ListFilesRequest")

        client = GoogleDriveFSClient(url)
        client._service = mocker.MagicMock()
        client._get_leaf_descriptor = mocker.MagicMock()
        client._get_leaf_descriptor.return_value = folder_descriptor

        with client:
            client.scandir()

        kwargs = lister.mock_calls[0][2]
        assert kwargs["url_base"] == "gdrive:/My Drive/folder/"

    def test_scandir_not_found(self, client):

        client._get_leaf_descriptor.side_effect = [IOError("🤷")]
        with pytest.raises(IOError), client:
            client.scandir()

    def test_remove(self, client, file_descriptor):

        client.remove()

        kwargs = client._service.files.return_value.delete.mock_calls[0][2]
        assert kwargs["fileId"] == file_descriptor.id_

    def test_remove_not_found(self, client):

        client._get_leaf_descriptor.side_effect = [IOError("test error")]
        with pytest.raises(IOError, match="test error"), client:
            client.remove()

    def test_download_not_found(self, client):
        client._get_leaf_descriptor.side_effect = [IOError("test error")]
        buff = io.BytesIO()
        with pytest.raises(IOError, match="test error"), client:
            client.get(buff)

    def test_download(self, mocker, client):

        data = b"some data"
        buff = io.BytesIO()

        def download():
            buff.write(data)
            return None, True

        media_download = mocker.patch("tentaclio.clients.google_drive_client.MediaIoBaseDownload")
        media_download.return_value.next_chunk.side_effect = download

        client.get(buff)
        buff.seek(0)
        assert buff.getvalue() == data

    def test_create_new_file_folder_not_found(self, mocker, client):
        client._get_path_descriptors.side_effect = [IOError("test error")]
        with pytest.raises(IOError, match="test error"), client:
            client._create(mocker.MagicMock())

    def test_create_new_file_parent_not_folder(self, mocker, client, file_descriptor):
        client._get_path_descriptors.return_value = [file_descriptor]
        with pytest.raises(IOError, match="parent path is not a folder"), client:
            client._create(mocker.MagicMock())

    def test_create_new_file(self, mocker, client, folder_descriptor):
        client._get_path_descriptors.return_value = [folder_descriptor]
        client._create(mocker.MagicMock())
        client._service.files.return_value.create.assert_called_once()

    def test_update_file(self, mocker, client, file_descriptor):
        client._update(file_descriptor, mocker.MagicMock())
        client._service.files.return_value.update.assert_called_once()

    def test_put_update(self, mocker, client):
        # by default the client comes with a valid
        # file descriptor
        client._update = mocker.MagicMock()
        reader = mocker.MagicMock()
        with client:
            client.put(reader)
        client._update.assert_called_once()

    def test_put_create(self, mocker, client):
        client._get_leaf_descriptor.side_effect = [IOError("🤷")]
        client._create = mocker.MagicMock()
        reader = mocker.MagicMock()
        with client:
            client.put(reader)
        client._create.assert_called_once()

    def test_get_leaf_descriptor(self, mocker, file_descriptor):
        file_descriptor_leaf = copy.copy(file_descriptor)
        file_descriptor_leaf.id_ = "leaf"

        client = GoogleDriveFSClient("gdrive:///My Drive/file")
        client._service = mocker.MagicMock()
        client._get_drives = mocker.MagicMock()
        client._get_drives.return_value = {
            "My Drive": GoogleDriveFSClient.DEFAULT_DRIVE_DESCRIPTOR
        }
        client._path_parts_to_descriptors = mocker.MagicMock()
        client._path_parts_to_descriptors.return_value = [folder_descriptor, file_descriptor_leaf]
        leaf = client._get_leaf_descriptor()
        assert leaf.id_ == "leaf"

    def test_get_file_descriptor_by_name_found_with_parent(
        self, client, mocked_service, file_props
    ):
        client._service = mocked_service
        f = client._get_file_descriptor_by_name("name", "parent")
        assert f.id_ == file_props["id"]
        kwargs = client._service.files.return_value.list.mock_calls[0][2]
        assert "parent" in kwargs["q"]

    def test_get_file_descriptor_by_name_found_without_parent(
        self, client, mocked_service, file_props
    ):
        client._service = mocked_service
        f = client._get_file_descriptor_by_name("name", None)

        assert f.id_ == file_props["id"]
        kwargs = mocked_service.files.return_value.list.mock_calls[0][2]
        assert "parent" not in kwargs["q"]

    def test_get_file_descriptor_by_name_not_found(self, client, mocked_service, file_props):
        client._service = mocked_service
        client._service.files.return_value.list.return_value.execute.return_value = {"files": []}
        with pytest.raises(IOError, match="Descriptor not found"):
            client._get_file_descriptor_by_name("name", None)

    def test_path_parts_to_descriptors(self, client, mocker):
        client._get_file_descriptor_by_name = mocker.MagicMock()
        client._get_file_descriptor_by_name.side_effect = [
            _GoogleFileDescriptor(
                id_=2,
                name="folder",
                parents=[1],
                mime_type=_GoogleFileDescriptor.FOLDER_MIME_TYPE,
                url="gdrive://My Drive/folder",
            ),
            _GoogleFileDescriptor(
                id_=3,
                name="inner",
                parents=[2],
                mime_type=_GoogleFileDescriptor.FOLDER_MIME_TYPE,
                url="gdrive://My Drive/folder/inner",
            ),
        ]

        descriptors = client._path_parts_to_descriptors(
            GoogleDriveFSClient.DEFAULT_DRIVE_DESCRIPTOR, ["folder", "inner"]
        )
        ids = [d.id_ for d in descriptors]
        assert ids == ["root", 2, 3]


class TestGoogleFileDescriptor:
    def test_is_dir(self):
        args = dict(
            name="file",
            id_="123",
            mime_type=_GoogleFileDescriptor.FOLDER_MIME_TYPE,
            parents=[],
            url="googledrive:///root/",
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
            url="googledrive:///root/",
        )
        descriptor = _GoogleFileDescriptor(**args)
        assert not descriptor.is_dir
        assert descriptor.is_file


class TestCreateRequest:
    def test_execute(self, mocker):
        service = mocker.MagicMock()
        reader = mocker.MagicMock()
        uploader = _CreateRequest(service, "name.txt", "parent_id", reader)
        uploader.execute()
        kwargs = service.files.return_value.create.mock_calls[0][2]
        assert kwargs["body"]["name"] == "name.txt"
        assert kwargs["body"]["parents"] == ["parent_id"]
        assert kwargs["media_body"].mimetype() == "text/plain"


class TestUpdateRequest:
    def test_execute(self, mocker):
        service = mocker.MagicMock()
        reader = mocker.MagicMock()
        uploader = _UpdateRequest(service, "123", "name.txt", reader)
        uploader.execute()
        kwargs = service.files.return_value.update.mock_calls[0][2]
        assert kwargs["fileId"] == "123"
        assert kwargs["media_body"].mimetype() == "text/plain"


class TestListFilesRequest:
    def test_build_descriptor(self, mocker, file_props):
        lister = _ListFilesRequest(mocker.Mock)
        descriptor = next(lister._build_descriptors([file_props]))
        assert descriptor.id_ == file_props["id"]
        assert descriptor.name == file_props["name"]
        assert descriptor.parents == file_props["parents"]
        assert descriptor.mime_type == file_props["mimeType"]

    def test_build_descriptor_with_url(self, mocker, file_props):
        lister = _ListFilesRequest(mocker.Mock, url_base="googledrive://my drive/")
        descriptor = next(lister._build_descriptors([file_props],))
        assert descriptor.id_ == file_props["id"]
        assert descriptor.name == file_props["name"]
        assert descriptor.parents == file_props["parents"]
        assert descriptor.mime_type == file_props["mimeType"]
        assert str(descriptor.url) == "googledrive://my drive/file"

    def test_list_no_pagination(self, mocker, file_props, mocked_service):
        lister = _ListFilesRequest(mocked_service)
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


class TestListDrivesRequest:
    def test_yielder(self, mocker, drive_props, file_descriptor):
        mocked_get_drive_root = mocker.patch(
            "tentaclio.clients.google_drive_client._get_drive_root"
        )
        mocked_get_drive_root.return_value = file_descriptor

        service = mocker.MagicMock()
        lister = _ListDrivesRequest(service)

        descriptor = next(lister._yielder({"drives": [drive_props]}))
        assert descriptor.id_ == drive_props["id"]
        assert descriptor.name == drive_props["name"]
        assert descriptor.root_descriptor == file_descriptor

    def test_yielder_permission_error(self, mocker, drive_props, file_descriptor):
        mocked_get_drive_root = mocker.patch(
            "tentaclio.clients.google_drive_client._get_drive_root"
        )
        mocked_get_drive_root.side_effect = [IOError("No files found while inspecting drive")]

        service = mocker.MagicMock()
        lister = _ListDrivesRequest(service)

        assert len(list(lister._yielder({"drives": [drive_props]}))) == 0


def test_get_random_parent_no_children(mocker):
    service = mocker.MagicMock()
    service.files.return_value.list.return_value.execute.return_value = {"files": []}
    with pytest.raises(IOError, match="No files found"):
        _get_random_parent(service, "drive")


def test_get_random_parent_no_parents(mocker, file_props):
    service = mocker.MagicMock()
    del file_props["parents"]
    service.files.return_value.list.return_value.execute.return_value = {"files": [file_props]}
    parent = _get_random_parent(service, "drive")
    assert parent == file_props["id"]


def test_get_random_parent(mocker, file_props):
    service = mocker.MagicMock()
    service.files.return_value.list.return_value.execute.return_value = {"files": [file_props]}
    parent = _get_random_parent(service, "drive")
    assert parent == file_props["parents"][0]


def test_get_drive_root_parent_not_found(mocker):
    mock_random_parent = mocker.patch("tentaclio.clients.google_drive_client._get_random_parent")
    mock_random_parent.return_value = "id"

    service = mocker.MagicMock()
    service.files.return_value.get.return_value.execute.return_value = None

    with pytest.raises(IOError, match=("Parent not found")):
        _get_drive_root(service, "drive")


def test_get_drive_root(mocker, file_props, file_descriptor):
    mock_random_parent = mocker.patch("tentaclio.clients.google_drive_client._get_random_parent")
    mock_random_parent.return_value = "id"

    service = mocker.MagicMock()

    root_props = file_props.copy()
    del root_props["parents"]
    root_props["id"] = "root"
    service.files.return_value.get.return_value.execute.side_effect = [file_props, root_props]

    descriptor = _get_drive_root(service, "drive")
    assert descriptor.id_ == "root"


def test_home_varible_set(mocker):
    """Test DEFAULT_TOKEN_FILE is correct."""
    env_dict = {"HOME": "/home"}
    expected_token_file = "/home/.tentaclio_google_drive.json"
    mocker.patch("os.environ", env_dict)
    assert _get_default_token_file() == expected_token_file


def test_home_varible_set_windows(mocker):
    """Test DEFAULT_TOKEN_FILE is correct."""
    env_dict = {"UserProfile": "/user-profile", "HOME": "/home"}
    expected_token_file = "/user-profile/.tentaclio_google_drive.json"
    mocker.patch("platform.system", lambda: "windows")
    mocker.patch("os.environ", env_dict)
    assert _get_default_token_file() == expected_token_file


def test_home_varible_set_no_home(mocker):
    """Test DEFAULT_TOKEN_FILE is correct."""
    expected_token_file = "/cwd/.tentaclio_google_drive.json"
    mocker.patch("os.getcwd", lambda: "/cwd")
    mocker.patch("os.environ", {})
    assert _get_default_token_file() == expected_token_file
