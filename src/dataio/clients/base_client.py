import abc
from typing import Iterable, Optional

from dataio import protocols
from dataio.urls import URL


class BaseClient(metaclass=abc.ABCMeta):
    """
    Abstract base class for clients, wrapping a connection
    """

    url: URL
    conn: Optional[protocols.Closable] = None

    def __init__(self, url: str) -> None:
        self.url = URL(url)

    # Context manager:

    def __enter__(self) -> "BaseClient":
        self.conn = self.connect()
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    # Connection methods:

    @abc.abstractmethod
    def connect(self) -> protocols.Closable:
        ...


class QueryClient(BaseClient):
    """
    Interface for query-based connections
    """

    # Query methods:

    @abc.abstractmethod
    def execute(self, sql_query: str, **params) -> None:
        ...

    @abc.abstractmethod
    def query(self, sql_query: str, **params) -> Iterable:
        ...


class StreamClient(BaseClient):
    """
    Interface for stream-based connections
    """

    # Stream methods:

    @abc.abstractmethod
    def get(self, **params) -> protocols.Reader:
        ...

    @abc.abstractmethod
    def put(self, file_obj: protocols.Writer, **params) -> None:
        ...
