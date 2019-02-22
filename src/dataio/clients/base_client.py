import abc
from typing import Generic, Iterable, TypeVar, Union

from .. import protocols, urls


T = TypeVar("T")


class BaseClient(Generic[T], metaclass=abc.ABCMeta):
    """Abstract base class for clients, wrapping a connection"""

    url: urls.URL
    conn: protocols.Closable
    allowed_schemes: Iterable[str] = []
    closed: bool = True

    def __init__(self, url: Union[urls.URL, str]) -> None:
        if isinstance(url, str):
            url = urls.URL(url)
        self.url = url
        if self.url.scheme not in self.allowed_schemes:
            raise ValueError(
                f"Allowed schemes for {type(self).__name__} are {self.allowed_schemes}; "
                f"found '{self.url.scheme}'"
            )

    # Context manager:

    @abc.abstractmethod
    def __enter__(self) -> T:
        ...

    def __exit__(self, *args) -> None:
        self.close()

    # Connection methods:

    def connect(self) -> protocols.Closable:
        self.closed = False
        return self._connect()

    @abc.abstractmethod
    def _connect(self) -> protocols.Closable:
        ...

    def close(self) -> None:
        """Close the client connection. """
        if self.closed:
            raise ValueError("Trying to close a closed client")

        self.conn.close()
        self.closed = True


class StreamClient(BaseClient["StreamClient"]):
    """Interface for stream-based connections"""

    # Context manager:

    def __enter__(self) -> "StreamClient":
        self.conn = self.connect()
        return self

    # Stream methods:

    @abc.abstractmethod
    def get(self, writer: protocols.ByteWriter, **params) -> None:
        ...

    @abc.abstractmethod
    def put(self, reader: protocols.ByteReader, **params) -> None:
        ...


class QueryClient(BaseClient["QueryClient"]):
    """Interface for query-based connections"""

    # Context manager:

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
