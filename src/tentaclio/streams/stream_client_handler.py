"""Base handler."""
import io
import logging
from typing import Callable

from typing_extensions import ContextManager

from tentaclio.protocols import ReaderClosable, WriterClosable
from tentaclio.streams import base_stream
from tentaclio.urls import URL


logger = logging.getLogger(__name__)

StreamerFactory = Callable[..., ContextManager[base_stream.Streamer]]

__all__ = ["StreamURLHandler"]


def _is_bytes_mode(mode: str) -> bool:
    if "b" in str(mode):
        return True
    return False


class StreamURLHandler:
    """Handler for opening writers and readers ."""

    client: StreamerFactory

    def __init__(self, client_factory: StreamerFactory):
        """Create a handler using  a stream client factory to instantiate the underlying client."""
        self.client_factory = client_factory

    def open_reader_for(self, url: URL, mode: str, extras: dict) -> ReaderClosable:
        """Open an stream client for reading."""
        client = self.client_factory(url, **extras)

        if _is_bytes_mode(mode):
            return base_stream.StreamerReader(client, io.BytesIO())
        return base_stream.StringToBytesClientReader(client)

    def open_writer_for(self, url: URL, mode: str, extras: dict) -> WriterClosable:
        """Open an stream client writing."""
        client = self.client_factory(url, **extras)

        if _is_bytes_mode(mode):
            return base_stream.StreamerWriter(client, io.BytesIO())
        return base_stream.StringToBytesClientWriter(client)
