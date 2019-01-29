import logging
from typing import ContextManager, Dict, Optional
from urllib import parse

from typing_extensions import Protocol

from dataio import protocols


__all__ = ["URLError", "URLHandlerRegistry", "URL"]

logger = logging.getLogger(__name__)


def open_reader(url: str, **kwargs) -> ContextManager[protocols.Reader]:
    """ Opens the url and returns a reader """
    return URL(url).open_reader(extras=kwargs)


def open_writer(url: str, **kwargs) -> ContextManager[protocols.Writer]:
    """ Opens the url and returns a writer"""
    return URL(url).open_writer(extras=kwargs)


class URLError(Exception):
    """
    Error encountered while processing a URL
    """


class URLHandler(Protocol):
    def open_reader_for(self, url: "URL", extras: dict) -> protocols.ReaderClosable:
        pass

    def open_writer_for(self, url: "URL", extras: dict) -> protocols.WriterClosable:
        pass


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

    _handler_registry: URLHandlerRegistry = URLHandlerRegistry()

    scheme: str
    username: Optional[str] = None
    password: Optional[str] = None
    hostname: Optional[str] = None
    port: Optional[int] = None
    path: str
    query: Optional[Dict[str, str]] = None

    def __init__(self, url: str) -> None:
        if url is None:
            raise URLError("URL is none")

        self.url = url
        self._parse_url(url)

    # Helpers:

    def _parse_url(self, url: str) -> None:
        parsed_url = parse.urlparse(url)
        if parsed_url.scheme not in self._handler_registry:
            raise URLError(f"URL: scheme {parsed_url.scheme} not supported")

        self.scheme = parsed_url.scheme
        self.username = parsed_url.username
        self.password = parsed_url.password
        self.hostname = parsed_url.hostname
        self.port = parsed_url.port
        self.path = parsed_url.path

        # Replace %xx escapes - ONLY for username & password
        if parsed_url.username:
            self.username = parse.unquote(self.username)
        if parsed_url.password:
            self.password = parse.unquote(self.password)

        if parsed_url.query:
            # Assuming string values
            self.query = {
                key: value for key, value in parse.parse_qsl(parsed_url.query, strict_parsing=True)
            }
        else:
            self.query = None

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
        return self.url

    def __repr__(self):
        return f"URL({self.url})"

    def open_reader(self, extras: Optional[dict] = None) -> ContextManager[protocols.Reader]:
        """Open a reader for the stream located at this url"""

        extras = extras or {}
        return self._handler_registry.get_handler(self.scheme).open_reader_for(self, extras)

    def open_writer(self, extras: Optional[dict] = None) -> ContextManager[protocols.Writer]:
        """Open a writer for the stream located at this url"""
        extras = extras or {}
        return self._handler_registry.get_handler(self.scheme).open_writer_for(self, extras)

    @classmethod
    def register_handler(cls, scheme: str, handler: URLHandler) -> None:
        """Register handler for the given protcol/scheme"""
        cls._handler_registry.register(scheme, handler)
