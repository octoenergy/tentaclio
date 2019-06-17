"""URL related clasess."""
import logging
from typing import Any, Dict, Optional
from urllib import parse


__all__ = ["URL"]

logger = logging.getLogger(__name__)


class URLError(Exception):
    """Error encountered while processing a URL."""


def _netloc_from_components(username=None, password=None, hostname=None, port=None):
    """Construct network location in accordance with urllib."""
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
    """Immutable class that give access to single url components and allows access to streams."""

    _scheme: str
    _username: Optional[str] = None
    _password: Optional[str] = None
    _hostname: Optional[str] = None
    _port: Optional[int] = None
    _path: str
    _query: Optional[Dict[str, str]] = None

    def __init__(self, url: str) -> None:
        """Create a url by parsing the parametre."""
        self._url = url
        self._parse_url()

    # Helpers:
    def _parse_url(self) -> None:
        if self._url is None:
            raise URLError("None url")

        parsed_url = parse.urlparse(self._url)

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

    def copy(
        self,
        scheme: str = None,
        username: str = None,
        password: str = None,
        hostname: str = None,
        port: int = None,
        path: str = None,
        query: str = None,
    ) -> "URL":
        """Copy this url optionally overwriting the provided components."""
        return URL.from_components(
            scheme=scheme or self.scheme,
            username=username or self.username,
            password=password or self.password,
            hostname=hostname or self.hostname,
            port=port or self.port,
            path=path or self.path,
            query=query or self.query,
        )

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
        """Return a string representation of the url hidding compromising credentials."""
        password = self.password
        if password:
            password = "__secret__" + password[-4:]
        return self.copy(password=password)._url

    def __eq__(self, other: Any):
        """Check if two urls are equal."""
        if not isinstance(other, URL):
            return False
        return self._url == other._url

    def __repr__(self):
        """Return the string url representation."""
        return f"URL({self._url})"

    @property
    def scheme(self) -> str:
        """Access the scheme."""
        return self._scheme

    @property
    def username(self) -> Optional[str]:
        """Access the username."""
        return self._username

    @property
    def password(self) -> Optional[str]:
        """Access the password."""
        return self._password

    @property
    def hostname(self) -> Optional[str]:
        """Access the hostname."""
        return self._hostname

    @property
    def port(self) -> Optional[int]:
        """Access the port."""
        return self._port

    @property
    def path(self) -> str:
        """Access the path."""
        return self._path

    @property
    def query(self) -> Optional[Dict[str, str]]:
        """Access the query."""
        return self._query

    @property
    def url(self) -> str:
        """Return the original url."""
        return self._url
