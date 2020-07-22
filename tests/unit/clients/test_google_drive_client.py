import pytest

from tentaclio.clients import GoogleDriveFSClient


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
