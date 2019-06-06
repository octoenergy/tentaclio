"""Base url handler registry."""
import logging
from typing import Dict, Generic, TypeVar


T = TypeVar("T")


logger = logging.getLogger(__name__)


class URLHandlerRegistry(Generic[T]):
    """Registry for url handlers based on the url scheme."""

    registry: Dict[str, T]

    def __init__(self):
        """Create a new empty registry."""
        self.registry = {}

    def register(self, scheme: str, url_handler: T):
        """Register the handler to the given scheme."""
        if scheme not in self.registry:
            logger.info(f"registering url scheme {scheme}")
            self.registry[scheme] = url_handler

    def get_handler(self, scheme: str) -> T:
        """Get the handler for the give scheme. Raise an URLError if no handler is registred."""
        if scheme not in self.registry:
            msg = f"Scheme {scheme} not found in the registry"
            logger.error(msg)
            raise KeyError(msg)
        return self.registry[scheme]

    def __contains__(self, scheme: str) -> bool:
        """Check that the scheme is in the registry."""
        return scheme in self.registry
