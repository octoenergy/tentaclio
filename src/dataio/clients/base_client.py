import abc
import typing
from urllib import parse

from . import exceptions


__all__ = ["BaseClient"]


SCHEMES = ("file", "s3", "postgres")


class URL:
    """
    Placeholder to process and store information for a given URL
    """

    scheme: typing.Optional[str]
    username: typing.Optional[str]
    password: typing.Optional[str]
    hostname: typing.Optional[str]
    port: typing.Optional[int]
    path: typing.Optional[str]

    def __init__(self, url: typing.Union[str, None]) -> None:
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
        if self.scheme in ("s3", "postgres"):
            if self.path != "":
                self.path = parsed_url.path[1:]


class BaseClient(metaclass=abc.ABCMeta):
    """
    Abstract base class for clients, wrapping a connection
    """

    url: URL
    conn = None

    def __init__(self, url: typing.Union[str, None]) -> None:
        self.url = URL(url)
        self.conn = None

    # Context manager

    def __enter__(self) -> "BaseClient":
        self.conn = self.get_conn()
        return self

    def __exit__(self, *args) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    # Connection methods

    @abc.abstractmethod
    def get_conn(self):
        raise NotImplementedError()
