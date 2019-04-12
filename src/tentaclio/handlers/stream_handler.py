"""Base handler."""
import io
from typing import Callable

from tentaclio.clients import base_client
from tentaclio.protocols import ReaderClosable, WriterClosable
from tentaclio.streams import base_stream
from tentaclio.urls import URL


StreamClientFactory = Callable[..., base_client.StreamClient]


def _is_bytes_mode(mode: str) -> bool:
    if "b" in str(mode):
        return True
    return False


class StreamURLHandler:
    """Handler for opening writers and readers ."""

    client: StreamClientFactory

    def __init__(self, client_factory: StreamClientFactory):
        """Create a handler using  a stream client factory to instantiate the underlying client."""
        self.client_factory = client_factory

    def open_reader_for(self, url: URL, mode: str, extras: dict) -> ReaderClosable:
        """Open an stream client for reading."""
        client = self.client_factory(url, **extras)

        if _is_bytes_mode(mode):
            return base_stream.StreamClientReader(client, io.BytesIO())
        return base_stream.StringToBytesClientReader(client)

    def open_writer_for(self, url: URL, mode: str, extras: dict) -> WriterClosable:
        """Open an stream client writing."""
        client = self.client_factory(url, **extras)

        if _is_bytes_mode(mode):
            return base_stream.StreamClientWriter(client, io.BytesIO())
        return base_stream.StringToBytesClientWriter(client)
