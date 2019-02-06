import io
from typing import Any, Callable

from dataio.clients import StreamClient, StreamClientReader, StreamClientWriter
from dataio.protocols import ReaderClosable, WriterClosable
from dataio.urls import URL

StreamClientFactory = Callable[..., StreamClient]


def is_bytes_mode(mode: str) -> bool:
    if "b" in str(mode):
        return True
    return False


def _get_buff(mode: str):
    buffer_factory: Any
    if is_bytes_mode(mode):
        return io.BytesIO()
    else:
        return io.StringIO()


class StreamURLHandler:
    """Handler for opening writers and readers ."""

    client: StreamClientFactory

    def __init__(self, client_factory: StreamClientFactory):
        self.client_factory = client_factory

    def open_reader_for(self, url: URL, mode: str, extras: dict) -> ReaderClosable:
        """Open an stream client for reading."""
        buffer = _get_buff(mode)
        return StreamClientReader(
            self.client_factory(url, **extras), buffer
        )

    def open_writer_for(self, url: URL, mode: str, extras: dict) -> WriterClosable:
        """Open an stream client writing."""
        buffer = _get_buff(mode)
        return StreamClientWriter(
            self.client_factory(url, **extras), buffer
        )
