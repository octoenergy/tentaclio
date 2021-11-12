import pytest

from tentaclio.clients.dbfs_client import DBFSClient


@pytest.mark.parametrize(
    "url, expected_path",
    [("dbfs:///test", "/dbfs/test"), ("dbfs:///path/to/file/", "/dbfs/path/to/file")],
)
def test_path_prefix(url: str, expected_path: str):
    client = DBFSClient(url)
    assert client.path == expected_path
