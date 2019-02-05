from dataio.clients import http_client

from .stream_handler import StreamURLHandler


__all__ = ["HTTPHandler"]


class HTTPHandler(StreamURLHandler):
    """Handler for opening writers and readers for http requests"""

    def __init__(self):
        super().__init__(http_client.HTTPClient)
