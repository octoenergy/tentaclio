import abc
import typing
from urllib import parse

__all__ = ["BaseHook"]


SCHEMES = ("file", "s3", "postgres")


class Credential:
    """
    Placeholder to process and store informations from a URI.
    """

    scheme: typing.Optional[str]
    username: typing.Optional[str]
    password: typing.Optional[str]
    hostname: typing.Optional[str]
    port: typing.Optional[int]
    path: typing.Optional[str]

    def __init__(self, uri: str = None) -> None:
        if uri is None:
            raise ValueError("Provide an URI to initialise a connection")

        self._parse_uri(uri)

    # Helpers:

    def _parse_uri(self, uri: str) -> None:
        parsed_uri = parse.urlparse(uri)
        assert parsed_uri.scheme in SCHEMES

        self.scheme = parsed_uri.scheme
        self.username = parsed_uri.username
        self.password = parsed_uri.password
        self.hostname = parsed_uri.hostname
        self.port = parsed_uri.port
        self.path = parsed_uri.path

        # Exception: hostname - S3
        if self.scheme == "s3":
            if self.hostname == "s3":
                self.hostname = None

        # Exception: path - S3 & Postgres
        if self.scheme in ("s3", "postgres"):
            if self.path != "":
                self.path = parsed_uri.path[1:]


class BaseHook(metaclass=abc.ABCMeta):
    """
    Abstract base class for hooks, wrapping a connection
    """

    @classmethod
    def get_credentials(cls, uri: str = None):
        cred = Credential(uri=uri)
        return cred

    # Context manager

    @abc.abstractmethod
    def __enter__(self) -> "BaseHook":
        raise NotImplementedError

    @abc.abstractmethod
    def __exit__(self) -> None:
        raise NotImplementedError
