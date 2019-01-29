import abc
import io
from typing import Any, Iterable, Optional, Union

from dataio import protocols
from dataio.urls import URL

__all__ = ["StreamClient", "StreamClientReader", "StreamClientWriter"]


class BaseClient(metaclass=abc.ABCMeta):
    """
    Abstract base class for clients, wrapping a connection
    """

    url: URL
    conn: Optional[protocols.Closable] = None

    def __init__(self, url: Union[URL, str]) -> None:
        if isinstance(url, str):
            url = URL(url)
        self.url = url

    # Context manager
    # no return type so child classes can define it

    @abc.abstractmethod
    def __enter__(self):
        pass

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
        print("CONNECTING !!!")
        self.conn = self.connect()
        print("My connection", self.conn)
        return self

    @abc.abstractmethod
    def get(self, **params) -> protocols.ReaderClosable:
        ...

    @abc.abstractmethod
    def put(self, file_obj: protocols.Writer, **params) -> None:
        ...


class StreamClientWriter(object):
    def __init__(self, client: StreamClient, buffer_factory=io.BytesIO):
        self.buffer = buffer_factory()
        self.client = client

    def write(self, contents: Any) -> int:
        return self.buffer.write(contents)

    def close(self) -> None:
        """Flush and close the writer"""
        self.buffer.seek(0)
        with self.client:
            self.client.put(self.buffer)
        self.buffer.close()


class StreamClientReader(object):
    buff: protocols.ReaderClosable

    def __init__(self, client: StreamClient):
        self.client = client
        self._load()

    def _load(self):
        with self.client:
            self.buffer = self.client.get()

    def read(self, size: int = -1) -> Any:
        """Read the contents of the buffer"""
        return self.buffer.read(size)

    def close(self) -> None:
        """Flush and close the writer"""
        self.buffer.close()
