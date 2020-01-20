"""Define the copiable protocol and registry."""
from typing import ClassVar

from typing_extensions import Protocol

from tentaclio.registry import URLHandlerRegistry
from tentaclio.urls import URL

from .copiers import DefaultCopier


__all__ = ["COPIER_REGISTRY"]


class Copier(Protocol):
    """Protocol of clients able to copy from the same type of client."""

    def copy(self, source: URL, dest: URL):
        """Copy from source url to dest url.

        This protocol allows clients to specialise the behaviour of copying different streams.
        """
        pass


class CopierRegistry(URLHandlerRegistry[Copier]):
    """Registry for scanners.

    The scheme expected by this registry is a compound one having
    the origin as first element and the destination as second element.
    i.e. s3+s3, file+s3, ...

    """

    def get_handler(self, scheme: str) -> Copier:
        """Get the handler for the given scheme.

        if the scheme is not set return the default copier.
        """
        if scheme not in self.registry:
            return DefaultCopier()
        return self.registry[scheme]


class _CopierRegistryHolder:
    """Module level singleton."""

    instance: ClassVar[CopierRegistry] = CopierRegistry()


COPIER_REGISTRY = _CopierRegistryHolder().instance
