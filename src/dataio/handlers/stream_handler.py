import io
from typing import Callable

from dataio.clients import (
    StreamClient,
    StreamClientReader,
    StreamClientWriter,
    StringToBytesClientReader,
    StringToBytesClientWriter
)
from dataio.protocols import ReaderClosable, WriterClosable
from dataio.urls import URL


StreamClientFactory = Callable[..., StreamClient]


def _is_bytes_mode(mode: str) -> bool:
    if "b" in str(mode):
        return True
    return False


class StreamURLHandler:
    """Handler for opening writers and readers ."""

    client: StreamClientFactory

    def __init__(self, client_factory: StreamClientFactory):
        self.client_factory = client_factory

    def open_reader_for(self, url: URL, mode: str, extras: dict) -> ReaderClosable:
        """Open an stream client for reading."""
        client = self.client_factory(url, **extras)

        if _is_bytes_mode(mode):
            return StreamClientReader(client, io.BytesIO())
        return StringToBytesClientReader(client)

    def open_writer_for(self, url: URL, mode: str, extras: dict) -> WriterClosable:
        """Open an stream client writing."""
        client = self.client_factory(url, **extras)

        if _is_bytes_mode(mode):
            return StreamClientWriter(client, io.BytesIO())
        return StringToBytesClientWriter(client)
