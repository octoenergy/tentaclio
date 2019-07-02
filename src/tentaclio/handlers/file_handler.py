"""Handler for local files.

Takes care of urls of the form '/tmp/myfile' and 'file:///tmp/myfile'.
"""
from tentaclio.clients import local_fs_client
from .stream_handler import StreamURLHandler


__all__ = ["LocalFileHandler"]


class LocalFileHandler(StreamURLHandler):
    """Handler for opening writers and readers for local files."""

    def __init__(self):
        """Create a handler for a file stream."""
        super().__init__(local_fs_client.LocalFSClient)
