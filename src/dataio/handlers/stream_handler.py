import io
from typing import Any, Callable

from dataio.clients import StreamClient, StreamClientReader, StreamClientWriter
from dataio.protocols import ReaderClosable, WriterClosable
from dataio.urls import URL


StreamClientFactory = Callable[..., StreamClient]


def _is_bytes_mode(mode: str) -> bool:
    if "b" in str(mode):
        return True
    return False


def _get_buff_factory(mode: str):
    buffer_factory: Any
    if _is_bytes_mode(mode):
        buffer_factory = io.BytesIO
    else:
        buffer_factory = io.StringIO
    return buffer_factory


class StreamURLHandler:
    """Handler for opening writers and readers ."""

    client: StreamClientFactory

    def __init__(self, client_factory: StreamClientFactory):
        self.client_factory = client_factory

    def open_reader_for(self, url: URL, mode: str, extras: dict) -> ReaderClosable:
        """Open an stream client for reading."""
        buffer_factory = _get_buff_factory(mode)
        print("reader buffer_factory", buffer_factory)
        return StreamClientReader(
            self.client_factory(url, **extras), buffer_factory=buffer_factory
        )

    def open_writer_for(self, url: URL, mode: str, extras: dict) -> WriterClosable:
        """Open an stream client writing."""
        buffer_factory = _get_buff_factory(mode)
        return StreamClientWriter(
            self.client_factory(url, **extras), buffer_factory=buffer_factory
        )
