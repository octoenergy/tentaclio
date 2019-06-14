from typing import Iterable

from tentaclio import URL
from tentaclio.credentials import load_credentials_injector
from tentaclio.credentials.env import add_credentials_from_env
from tentaclio.fs import SCANNER_REGISTRY, api
from tentaclio.fs.scanner import DirEntry


class FakeScanner(object):
    def scandir(self, url: URL) -> Iterable[DirEntry]:
        self.scanned_url = url
        return [
            DirEntry(url=URL("scantest://mytest/file.txt"), is_dir=False, is_file=True),
            DirEntry(url=URL("scantest://mytest/myfolder"), is_dir=False, is_file=True),
        ]


def test_authenticate(mocker):
    scanner = FakeScanner()
    add_credentials_from_env(
        load_credentials_injector(),
        {"TENTACLIO__CONN__TEST": "scantest://costantine:tentacl3@mytest/"},
    )
    SCANNER_REGISTRY.register("scantest", scanner)
    api.scandir("scantest://mytest/")

    url = scanner.scanned_url
    assert url.username == "costantine"
    assert url.password == "tentacl3"


def test_scandir():
    scanner = FakeScanner()
    SCANNER_REGISTRY.register("scantest", scanner)
    entries = list(api.scandir("scantest://mytest/"))
    assert len(entries) == 2
    assert entries[0].url == URL("scantest://mytest/file.txt")
    assert entries[1].url == URL("scantest://mytest/myfolder")
