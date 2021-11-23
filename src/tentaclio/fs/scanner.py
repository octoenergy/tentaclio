"""Functionality for listing directory-like urls."""
from typing import ClassVar, Iterable, Protocol

from tentaclio.registry import URLHandlerRegistry
from tentaclio.urls import URL


__all__ = ["SCANNER_REGISTRY", "DirEntry", "build_file_entry", "build_folder_entry"]


class DirEntry:
    """Entry containing information about a directory scan item."""

    def __init__(self, *, url: URL, is_dir: bool, is_file: bool):
        """Create new dir entry."""
        self.url: URL = url
        self.is_dir: bool = is_dir
        self.is_file: bool = is_file


def build_folder_entry(url: URL) -> DirEntry:
    """Build a DirEntry with is_dir = True."""
    return DirEntry(url=url, is_file=False, is_dir=True)


def build_file_entry(url: URL) -> DirEntry:
    """Build a DirEntry with is_file = True."""
    return DirEntry(url=url, is_file=True, is_dir=False)


class Scanner(Protocol):
    """Scan a directory-like url."""

    def scandir(self, url: URL) -> Iterable[DirEntry]:
        """Scan a directory-like url returning its entries.

        Return an iterator of tentaclio.fs.DirEntry objects corresponding
        to the entries in the directory given by url. The entries
        are yielded in arbitrary order,
        and the special entries '.' and '..' are not included.

        More info:
        https://docs.python.org/3/library/os.html#os.scandir
        """
        ...


class ScannerRegistry(URLHandlerRegistry[Scanner]):
    """Registry for scanners."""

    ...


class _ScannerRegistryHolder:
    """Module level singleton."""

    instance: ClassVar[ScannerRegistry] = ScannerRegistry()


SCANNER_REGISTRY = _ScannerRegistryHolder().instance
