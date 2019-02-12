import abc
from typing import Generic, Iterable, Optional, TypeVar, Union

from .. import protocols, urls

T = TypeVar("T")


class BaseClient(Generic[T], metaclass=abc.ABCMeta):
    """
    Abstract base class for clients, wrapping a connection
    """

    url: urls.URL
    conn: Optional[protocols.Closable] = None
    allowed_schemes: Iterable[str] = []

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

    @abc.abstractmethod
    def connect(self) -> protocols.Closable:
        ...

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None


class QueryClient(BaseClient["QueryClient"]):
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
