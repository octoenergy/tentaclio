from dataio.clients import http_client
from dataio.protocols import WriterClosable
from dataio.urls import URL

from .stream_handler import StreamURLHandler

__all__ = ["HTTPHandler"]


class HTTPHandler(StreamURLHandler):
    """Handler for opening writers and readers for http requests"""

    def __init__(self):
        super().__init__(http_client.HTTPClient)

    def open_writer_for(self, url: URL, extras: dict) -> WriterClosable:
        raise NotImplementedError("Posting readable via HTTP/HTTPS not implemented")
