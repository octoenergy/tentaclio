import logging
from typing import Any, ClassVar, ContextManager, Dict, Optional
from urllib import parse

from typing_extensions import Protocol

from dataio import protocols


__all__ = ["URLError", "URLHandlerRegistry", "URL"]

logger = logging.getLogger(__name__)


class URLError(Exception):
    """
    Error encountered while processing a URL
    """


class URLHandler(Protocol):
    def open_reader_for(self, url: "URL", mode: str, extras: dict) -> protocols.ReaderClosable:
        ...

    def open_writer_for(self, url: "URL", mode: str, extras: dict) -> protocols.WriterClosable:
        ...


class URLHandlerRegistry(object):
    registry: Dict[str, URLHandler]

    def __init__(self):
        self.registry = {}

    def register(self, scheme: str, url_handler: URLHandler):
        if scheme not in self.registry:
            logger.info("registering url scheme {scheme}")
            self.registry[scheme] = url_handler

    def get_handler(self, scheme: str):
        if scheme not in self.registry:
            msg = f"Scheme {scheme} not found in the registry"
            logger.error(msg)
            raise URLError(msg)
        return self.registry[scheme]

    def __contains__(self, scheme: str) -> bool:
        return scheme in self.registry


class _ReaderContextManager(ContextManager[protocols.Reader]):
    """Context manager for ReaderCloseables"""

    def __init__(self, resource: protocols.ReaderClosable):
        super(_ReaderContextManager, self).__init__()
        self.resource = resource

    def __enter__(self) -> protocols.Reader:
        return self.resource

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return self.resource.close()


class _WriterContextManager(ContextManager[protocols.Writer]):
    """Context manager for ReaderCloseables"""

    def __init__(self, resource: protocols.WriterClosable):
        super(_WriterContextManager, self).__init__()
        self.resource = resource

    def __enter__(self) -> protocols.Writer:
        return self.resource

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return self.resource.close()


def _netloc_from_components(username=None, password=None, hostname=None, port=None):
    """Constract network location in accordance with urllib."""
    netloc = ""
    if username is not None:
        netloc += parse.quote(username, safe="")
        if password is not None:
            netloc += ":" + parse.quote(password, safe="")
        netloc += "@"
    hostname = hostname or ""
    netloc += hostname
    if port is not None:
        netloc += ":" + str(port)
    return netloc


class URL:
    """
    Placeholder to process and store information for a given URL
    """

    _handler_registry: ClassVar[URLHandlerRegistry] = URLHandlerRegistry()

    _scheme: str
    _username: Optional[str] = None
    _password: Optional[str] = None
    _hostname: Optional[str] = None
    _port: Optional[int] = None
    _path: str
    _query: Optional[Dict[str, str]] = None

    def __init__(self, url: str) -> None:
        self._url = url
        self._parse_url()

    # Helpers:

    def _parse_url(self) -> None:
        parsed_url = parse.urlparse(self._url)
        if parsed_url.scheme not in self._handler_registry:
            raise URLError(f"URL: scheme {parsed_url.scheme} not supported")

        self._scheme = parsed_url.scheme
        self._username = parsed_url.username
        self._password = parsed_url.password
        self._hostname = parsed_url.hostname
        self._port = parsed_url.port
        self._path = parsed_url.path

        # Replace %xx escapes - ONLY for username & password
        if parsed_url.username:
            self._username = parse.unquote(self._username)
        if parsed_url.password:
            self._password = parse.unquote(self._password)

        if parsed_url.query:
            # Assuming string values
            self._query = {
                key: value for key, value in parse.parse_qsl(parsed_url.query, strict_parsing=True)
            }
        else:
            self._query = None

    @classmethod
    def from_components(
        cls,
        scheme=None,
        username=None,
        password=None,
        hostname=None,
        port=None,
        path=None,
        query=None,
    ):
        """Construct a URL object from parts."""
        netloc = _netloc_from_components(
            username=username, password=password, hostname=hostname, port=port
        )
        params = None
        if query:
            query = parse.urlencode(query)
        fragment = None
        components = (scheme, netloc, path, params, query, fragment)
        url = parse.urlunparse(components)
        return URL(url)

    def __str__(self):
        return self._url

    def __eq__(self, other: Any):
        if not isinstance(other, URL):
            return False
        return self._url == other._url

    def __repr__(self):
        return f"URL({self._url})"

    @property
    def scheme(self) -> str:
        return self._scheme

    @property
    def username(self) -> Optional[str]:
        return self._username

    @property
    def password(self) -> Optional[str]:
        return self._password

    @property
    def hostname(self) -> Optional[str]:
        return self._hostname

    @property
    def port(self) -> Optional[int]:
        return self._port

    @property
    def path(self) -> str:
        return self._path

    @property
    def query(self) -> Optional[Dict[str, str]]:
        return self._query

    @property
    def url(self) -> str:
        return self._url

    def open_reader(
        self, mode: str, extras: Optional[dict] = None
    ) -> ContextManager[protocols.Reader]:

        """Open a reader for the stream located at this url"""

        extras = extras or {}
        reader = self._handler_registry.get_handler(self._scheme).open_reader_for(
            self, mode, extras
        )
        return _ReaderContextManager(reader)

    def open_writer(
        self, mode: str, extras: Optional[dict] = None
    ) -> ContextManager[protocols.Writer]:
        """Open a writer for the stream located at this url"""
        extras = extras or {}
        writer = self._handler_registry.get_handler(self._scheme).open_writer_for(
            self, mode, extras
        )
        return _WriterContextManager(writer)

    @classmethod
    def register_handler(cls, scheme: str, handler: URLHandler) -> None:
        """Register handler for the given protcol/scheme"""
        cls._handler_registry.register(scheme, handler)
