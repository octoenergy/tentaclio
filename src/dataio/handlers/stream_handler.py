from typing import Callable

from dataio.clients import StreamClient, StreamClientReader, StreamClientWriter
from dataio.protocols import ReaderClosable, WriterClosable
from dataio.urls import URL


StreamClientFactory = Callable[..., StreamClient]


class StreamURLHandler:
    """Handler for opening writers and readers ."""

    client: StreamClientFactory

    def __init__(self, client_factory: StreamClientFactory):
        self.client_factory = client_factory

    def open_reader_for(self, url: URL, extras: dict) -> ReaderClosable:
        """Open an stream client for reading."""
        return StreamClientReader(self.client_factory(url, **extras))

    def open_writer_for(self, url: URL, extras: dict) -> WriterClosable:
        """Open an stream client writing."""
        return StreamClientWriter(self.client_factory(url, **extras))
