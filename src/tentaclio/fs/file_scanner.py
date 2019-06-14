"""File Scanner."""
import logging
import os
from typing import Iterator

from tentaclio.urls import URL

from .scanner import DirEntry


logger = logging.getLogger(__name__)

__all__ = ["LocalFileScanner"]


def _from_os_dir_entry(original: os.DirEntry) -> DirEntry:
    return DirEntry(
        url=URL("file://" + os.path.abspath(original.path)),
        is_dir=bool(original.is_dir()),
        is_file=bool(original.is_file()),
    )


class LocalFileScanner:
    """Scan directories in the file system."""

    def scandir(self, url: URL) -> Iterator[DirEntry]:
        """Scan a local file system path and returns the dir entries."""
        path = url.path

        os_dir_entries = os.scandir(path)
        return map(_from_os_dir_entry, os_dir_entries)
