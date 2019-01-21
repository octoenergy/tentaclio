import abc
from typing import Iterable, Optional, Union
from urllib import parse

from dataio import protocols

from . import exceptions

SCHEMES = ("file", "s3", "postgresql", "ftp", "sftp")


def _netloc_from_parts(
    username=None,
    password=None,
    hostname=None,
    port=None
):
    """Constract network location in accordance with urllib."""
    netloc = ''
    if username is not None:
        netloc += parse.quote(username, safe='')
        if password is not None:
            netloc += ':' + parse.quote(password, safe='')
        netloc += '@'
    hostname = hostname or ''
    netloc += hostname
    if port is not None:
        netloc += ':' + str(port)
    return netloc


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
        self.url = url
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
        self.query = parse.parse_qs(parsed_url.query)

        # Replace %xx escapes - ONLY for username & password
        if parsed_url.username is not None:
            self.username = parse.unquote(parsed_url.username)
        if parsed_url.password is not None:
            self.password = parse.unquote(parsed_url.password)

    @classmethod
    def from_parts(
        cls,
        scheme=None,
        username=None,
        password=None,
        hostname=None,
        port=None,
        path=None,
        query=None
    ):
        """Construct a URL object from parts."""
        netloc = _netloc_from_parts(
            username=username, password=password, hostname=hostname, port=port
        )
        params = None
        query = parse.urlencode(query)
        fragment = None
        components = (scheme, netloc, path, params, query, fragment)
        url = parse.urlunparse(components)
        return URL(url)

    def __str__(self):
        return self.url
    
    def __repr__(self):
        return f'URL({self.url})'


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
