import json
import tempfile

import pytest

from tentaclio.clients import GoogleDriveFSClient
from tentaclio.clients.google_drive_client import _load_credentials


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
