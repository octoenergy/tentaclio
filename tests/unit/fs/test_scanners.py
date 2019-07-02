from typing import Iterable

from tentaclio import URL
from tentaclio.fs.scanner import DirEntry
from tentaclio.fs.scanners import ClientDirScanner


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
