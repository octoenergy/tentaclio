"""Clients protocols."""
import abc
from typing import Iterable

from typing_extensions import Protocol

from tentaclio import protocols


__all__ = ["Streamer", "DirScanner"]


class Streamer(Protocol):
    """Interface for stream-based connections."""

    # Stream methods:

    @abc.abstractmethod
    def get(self, writer: protocols.ByteWriter, **params) -> None:
        """Read the contents from the stream and write them the the ByteWriter."""
        ...

    @abc.abstractmethod
    def put(self, reader: protocols.ByteReader, **params) -> None:
        """Write the contents of the reader into the client stream."""
        ...


class DirScanner(Protocol):
    """DirScanner."""

    @abc.abstractmethod
    def scan_dir(self, **params) -> None:
        """Scan dir-like urls."""
        ...


class QueryClient(Protocol):
    """Interface for query-based connections."""

    # Query methods:

    @abc.abstractmethod
    def execute(self, sql_query: str, **params) -> None:
        """Execute a query against the underlying client."""
        ...

    @abc.abstractmethod
    def query(self, sql_query: str, **params) -> Iterable:
        """Perform the query and return an iterable of the results."""
        ...
