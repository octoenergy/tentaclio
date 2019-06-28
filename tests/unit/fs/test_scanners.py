import collections
from typing import Iterable

from tentaclio import URL
from tentaclio.fs.scanner import DirEntry
from tentaclio.fs.scanners import ClientDirScanner, LocalFileScanner


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
        entries = LocalFileScanner().scandir(scandir)
        for entry, expected in zip(entries, expected_values):
            assert entry.url == expected.url
            assert entry.is_dir == expected.is_dir
            assert entry.is_file == expected.is_file


class FakeClient:
    def __init__(self, url: URL):
        self.url = url
        self.entered = False

    def __enter__(self) -> "FakeClient":
        self.entered = True
        return self

    def __exit__(self, *args):
        pass

    def scandir(self) -> Iterable[DirEntry]:
        assert self.entered  # Connection should be open before scanning
        return (DirEntry(url=URL("file:///home/constantine"), is_dir=True, is_file=False),)


def test_client_scanner():
    scanner = ClientDirScanner(FakeClient)
    entries = scanner.scandir(URL("file:///home/"))
    assert entries[0].url == URL("file:///home/constantine")
