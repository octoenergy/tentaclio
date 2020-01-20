"""Define the copiable protocol and registry."""
from typing import ClassVar

from typing_extensions import Protocol

from tentaclio.registry import URLHandlerRegistry
from tentaclio.urls import URL


__all__ = ["COPIER_REGISTRY"]


class Copier(Protocol):
    """Protocol of clients able to copy from the same type of client."""

    def copy(self, source: URL, dest: URL):
        """Copy from source url to dest url.

        This protocol allows clients to specialise the behaviour of copying different streams.
        """
        pass


class CopierRegistry(URLHandlerRegistry[Copier]):
    """Registry for scanners."""

    ...


class _CopierRegistryHolder:
    """Module level singleton."""

    instance: ClassVar[CopierRegistry] = CopierRegistry()


COPIER_REGISTRY = _CopierRegistryHolder().instance
