"""Concrete implementations of dir scanners."""
import logging
from typing import Callable, ContextManager, Iterable, Protocol

from tentaclio.urls import URL

from .scanner import DirEntry


logger = logging.getLogger(__name__)

__all__ = ["ClientDirScanner"]


class ManagedDirScanner(ContextManager, Protocol):
    """Connection based dir scanner."""

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
