import abc
from typing import Iterable, Optional, Union

from dataio import protocols, urls


class BaseClient(metaclass=abc.ABCMeta):
    """
    Abstract base class for clients, wrapping a connection
    """

    url: urls.URL
    conn: Optional[protocols.Closable] = None

    def __init__(self, url: Union[urls.URL, str]) -> None:
        if isinstance(url, str):
            url = urls.URL(url)
        self.url = url

    # Context manager
    # no return type so child classes can define it

    @abc.abstractmethod
    def __enter__(self):
        ...

    def __exit__(self, *args) -> None:
        self.close()

    # Connection methods:

    @abc.abstractmethod
    def connect(self) -> protocols.Closable:
        ...

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None


class QueryClient(BaseClient):
    """
    Interface for query-based connections
    """

    def __enter__(self) -> "QueryClient":
        self.conn = self.connect()
        return self

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

    def __enter__(self) -> "StreamClient":
        self.conn = self.connect()
        return self

    @abc.abstractmethod
    def get(self, **params) -> protocols.ReaderClosable:
        ...

    @abc.abstractmethod
    def put(self, file_obj: protocols.Writer, **params) -> None:
        ...
