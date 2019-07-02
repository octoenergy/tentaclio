import collections

from tentaclio import URL
from tentaclio.clients.local_fs_client import LocalFSClient
from tentaclio.fs.scanner import DirEntry


FakeOsDirEntry = collections.namedtuple("DirEntry", ["name", "path", "is_dir", "is_file"])


class TestLocalFileScanner(object):
    def test_scan_dir(self, mocker):
        scandir = mocker.patch("os.scandir")
        scandir.return_value = [
            FakeOsDirEntry(
                name="file.txt",
                path="/home/costantine/file.txt",
                is_dir=lambda: False,
                is_file=lambda: True,
            ),
            FakeOsDirEntry(
                name=".ssh",
                path="/home/costantine/.ssh",
                is_dir=lambda: True,
                is_file=lambda: False,
            ),
        ]
        expected_values = [
            DirEntry(url=URL("file:///home/costantine/file.txt"), is_dir=False, is_file=True),
            DirEntry(url=URL("file:///home/costantine/.ssh"), is_dir=True, is_file=False),
        ]
        entries = LocalFSClient("file://home/costantine").scandir()
        for entry, expected in zip(entries, expected_values):
            assert entry.url == expected.url
            assert entry.is_dir == expected.is_dir
            assert entry.is_file == expected.is_file
