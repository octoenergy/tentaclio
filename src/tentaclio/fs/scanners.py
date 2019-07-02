"""Concrete implementations of dir scanners."""
import logging
import os
from typing import Callable, Iterable

from typing_extensions import Protocol

from tentaclio.urls import URL

from .scanner import DirEntry


logger = logging.getLogger(__name__)

__all__ = ["LocalFileScanner", "ClientDirScanner"]


def _from_os_dir_entry(original: os.DirEntry) -> DirEntry:
    return DirEntry(
        url=URL("file://" + os.path.abspath(original.path)),
        is_dir=bool(original.is_dir()),
        is_file=bool(original.is_file()),
    )


class LocalFileScanner:
    """Scan directories in the file system."""

    def scandir(self, url: URL) -> Iterable[DirEntry]:
        """Scan a local file system path and returns the dir entries."""
        path = url.path

        os_dir_entries = os.scandir(path)
        return map(_from_os_dir_entry, os_dir_entries)


class ManagedDirScanner(Protocol):
    """Connection based dir scanner."""

    # The context manager methods are included as we can't
    # inherit from typing_extensions.ContextManager and Protocol
    # at the same time 🤷
    def __enter__(self) -> "ManagedDirScanner":
        """Enter the the context manager."""
        ...

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Exit the the context manager."""
        ...

    def scandir(self, **params) -> Iterable[DirEntry]:
        """Scan based on a connection rather than a url."""
        ...


ManagedDirScannerFactory = Callable[..., ManagedDirScanner]


class ClientDirScanner:
    """DirScanner that is using a tentaclio client to scan dirs."""

    def __init__(self, client_factory: ManagedDirScannerFactory):
        """Create a new client dir scanner.

        The spected argument is class that has a context manager and a scandir method.
        i.e. tentaclio.clietns.s3_client.S3Client
        """
        self.client_factory = client_factory

    def scandir(self, url: URL) -> Iterable[DirEntry]:
        """Scan the dir-like url using the client factory to create a connection."""
        with self.client_factory(url) as client:
            return client.scandir()
