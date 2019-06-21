"""Main entry points for fs/os like operations."""
from typing import Iterable

from tentaclio import credentials

from .scanner import SCANNER_REGISTRY, DirEntry


__all__ = ["scandir", "listdir"]


def scandir(url: str) -> Iterable[DirEntry]:
    """Scan a directory-like url returning its entries.

    Return an iterator of tentaclio.fs.DirEntry objects corresponding
    to the entries in the directory given by url. The entries
    are yielded in arbitrary order,
    and the special entries '.' and '..' are not included.

    More info:
    https://docs.python.org/3/library/os.html#os.scandir
    """
    authenticated = credentials.authenticate(url)
    return SCANNER_REGISTRY.get_handler(authenticated.scheme).scandir(authenticated)


def listdir(url: str) -> Iterable[str]:
    """List a directory-like url returning its entries.

    Return an iterator string containing the urls
    to the entries in the directory given by url passed as parametre. The entries
    are yielded in arbitrary order,
    and the special entries '.' and '..' are not included.

    More info:
    https://docs.python.org/3/library/os.html#os.listdir
    """
    entries = scandir(url)
    return (str(entry.url) for entry in entries)
