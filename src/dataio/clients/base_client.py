import abc
from typing import Iterable, Optional, Union
from urllib import parse

from dataio import protocols

from . import exceptions

SCHEMES = ("file", "s3", "postgresql", "ftp", "sftp")


class URL:
    """
    Placeholder to process and store information for a given URL
    """

    scheme: str
    path: str
    hostname: Optional[str]
    port: Optional[int]
    username: Optional[str] = None
    password: Optional[str] = None

    def __init__(self, url: Union[str, None]) -> None:
        if url is None:
            raise exceptions.URIError("Provide an URI to initialise a connection")

        self._parse_url(url)

    # Helpers:

    def _parse_url(self, url: str) -> None:
        parsed_url = parse.urlparse(url)
        if parsed_url.scheme not in SCHEMES:
            raise exceptions.URIError("URI scheme currently not implemented")

        self.scheme = parsed_url.scheme
        self.hostname = parsed_url.hostname
        self.port = parsed_url.port
        self.path = parsed_url.path

        # Replace %xx escapes - ONLY for username & password
        if parsed_url.username is not None:
            self.username = parse.unquote(parsed_url.username)
        if parsed_url.password is not None:
            self.password = parse.unquote(parsed_url.password)


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
        self.conn = self.get_conn()
        return self

    def __exit__(self, *args) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    # Connection methods:

    @abc.abstractmethod
    def get_conn(self) -> protocols.Closable:
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
