"""Base defines a common contract for connecting, closing, managing context for clients."""
import abc
from typing import Container, ContextManager, TypeVar, Union, cast

from tentaclio import protocols, urls


T = TypeVar("T")


class BaseClient(ContextManager[T], metaclass=abc.ABCMeta):
    """Abstract base class for clients, wrapping a connection.

    Offers context managing, connecting and closing functionalities.
    """

    url: urls.URL
    conn: protocols.Closable
    allowed_schemes: Container[str] = []
    closed: bool = True

    def __init__(self, url: Union[urls.URL, str]) -> None:
        """Create a new client based on a URL object or a string containing a url."""
        if isinstance(url, str):
            url = urls.URL(url)
        self.url = url
        if self.url.scheme not in self.allowed_schemes:
            raise ValueError(
                f"Allowed schemes for {type(self).__name__} are {self.allowed_schemes}; "
                f"found '{self.url.scheme}'"
            )

    # Context manager:

    def __enter__(self) -> T:
        """Connect to the resource when entering the context."""
        self.conn = self.connect()
        return cast(T, self)

    def __exit__(self, *args) -> None:
        """Close the connection when leaving the context."""
        self.close()

    # Connection methods:

    def connect(self) -> protocols.Closable:
        """Connect to the resource when entering the context."""
        self.closed = False
        return self._connect()

    @abc.abstractmethod
    def _connect(self) -> protocols.Closable:
        """Connect to the resource. This method needs to be overwritten by child classes."""
        ...

    def close(self) -> None:
        """Close the client connection."""
        if not self.closed:
            self.conn.close()
            self.closed = True


class _LazyExceptionRaiser:
    """Object that raises the encapsulated exception when interacted with."""

    def __init__(self, exception: Exception):
        """Init a new LazyExceptionRaiser that will raise the passed exception."""
        self.exception = exception

    def _raise(self):
        raise self.exception

    def __call__(self, *args, **kwargs):
        self._raise()

    def __getattr__(self, name):
        return _LazyExceptionRaiser(self.exception)


class _ExtraPackageNeededException(Exception):
    def __init__(self, extra_name: str, original_error: Exception):
        self.extra_name = extra_name
        self.original_error = original_error

    def __str__(self) -> str:
        return f"""Please install extra requirement '{self.extra_name}' in order to give support that scheme

    pip install tentaclio[{self.extra_name}]

    The original missing import exception was :
    {self.original_error}
    """


def lazy_import_error(extra_name: str, original_error: Exception) -> _LazyExceptionRaiser:
    """Return object that will raise an Exception informing about the missing package.

    This is used to create lazy errors for schemas than need extra depenendencies to be installed
    """
    return _LazyExceptionRaiser(_ExtraPackageNeededException(extra_name, original_error))
