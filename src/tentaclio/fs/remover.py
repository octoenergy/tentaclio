"""Functionality for listing directory-like urls."""
from typing import Callable, ClassVar

from typing_extensions import Protocol

from tentaclio.registry import URLHandlerRegistry
from tentaclio.urls import URL


__all__ = ["REMOVER_REGISTRY", "ClientRemover"]


class Remover(Protocol):
    """Contract to delete the resource."""

    # The context manager methods are included as we can't
    # inherit from typing_extensions.ContextManager and Protocol
    # at the same time 🤷
    def __enter__(self) -> "Remover":
        """Enter the the context manager."""
        ...

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Exit the the context manager."""
        ...

    def remove(self):
        """Remove the underlying resource."""
        ...


class ClientRemover:
    """Wraps the client creation to get the remover."""

    def __init__(self, client_factory: Callable[..., Remover]):
        """Create the client remover."""
        self.client_factory = client_factory

    def remove(self, url: URL):
        """Build the client remover."""
        with self.client_factory(url) as client:
            client.remove()


class RemoverRegistry(URLHandlerRegistry[ClientRemover]):
    """Registry for scanners."""

    ...


class _RemoverRegistryHolder:
    """Module level singleton."""

    instance: ClassVar[RemoverRegistry] = RemoverRegistry()


REMOVER_REGISTRY = _RemoverRegistryHolder().instance
