import abc
from typing import Iterable, Optional
from urllib import parse

from . import exceptions, types

SCHEMES = ("file", "s3", "postgresql", "ftp", "sftp")


class URL:
    """
    Placeholder to process and store information for a given URL
    """

    scheme: Optional[str]
    username: Optional[str]
    password: Optional[str]
    hostname: Optional[str]
    port: Optional[int]
    path: Optional[str]

    def __init__(self, url: types.NoneString) -> None:
        if url is None:
            raise exceptions.URIError("Provide an URI to initialise a connection")

        self._parse_url(url)

    # Helpers:

    def _parse_url(self, url: str) -> None:
        parsed_url = parse.urlparse(url)
        if parsed_url.scheme not in SCHEMES:
            raise exceptions.URIError("URI scheme currently not implemented")

        self.scheme = parsed_url.scheme
        self.username = parsed_url.username
        self.password = parsed_url.password
        self.hostname = parsed_url.hostname
        self.port = parsed_url.port
        self.path = parsed_url.path

        # Exception: hostname - S3
        if self.scheme == "s3":
            if self.hostname == "s3":
                self.hostname = None

        # Exception: path - S3 & Postgres
        if self.scheme in ("s3", "postgresql"):
            if self.path != "":
                self.path = parsed_url.path[1:]


class BaseClient(metaclass=abc.ABCMeta):
    """
    Abstract base class for clients, wrapping a connection
    """

    url: URL
    conn: Optional[types.Closable]

    def __init__(self, url: types.NoneString) -> None:
        self.url = URL(url)
        self.conn = None

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
    def get_conn(self) -> types.Closable:
        raise NotImplementedError()


class QueryableMixin:
    """
    Interface for sequal-based connections
    """

    # Sequal methods:

    def execute(self, sql_query: str, **params) -> None:
        raise NotImplementedError

    def query(self, sql_query: str, **params) -> Iterable:
        raise NotImplementedError


class ReadableMixing:
    """
    Interface for document-based connections
    """

    # Document methods:

    def get(self, **params) -> types.T:
        raise NotImplementedError

    def put(self, **params) -> None:
        raise NotImplementedError
