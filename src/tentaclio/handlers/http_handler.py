"""Handler of http urls."""
from tentaclio.clients import http_client
from tentaclio.protocols import WriterClosable
from tentaclio.urls import URL

from .stream_handler import StreamURLHandler


__all__ = ["HTTPHandler"]


class HTTPHandler(StreamURLHandler):
    """Handler for opening writers and readers for http requests."""

    def __init__(self):
        """Create a http handler."""
        super().__init__(http_client.HTTPClient)

    def open_writer_for(self, url: URL, mode: str, extras: dict) -> WriterClosable:
        """Not implemented yet."""
        raise NotImplementedError("Posting readable via HTTP/HTTPS not implemented")
