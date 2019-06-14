"""Main entry points for fs/os like operations."""
from tentaclio import credentials
from typing import Iterator

from .scanner import SCANNER_REGISTRY, DirEntry


def scandir(url: str) -> Iterator[DirEntry]:
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
