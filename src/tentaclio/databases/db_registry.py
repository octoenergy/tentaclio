"""Stream handler registry to open readers and writers to urls."""
import abc
import logging
from typing import Callable, ClassVar, ContextManager, Iterable, Protocol

from tentaclio import clients
from tentaclio.registry import URLHandlerRegistry


logger = logging.getLogger(__name__)

__all__ = ["DB_REGISTRY", "Db"]


class Db(ContextManager, Protocol):
    """Interface for query-based connections."""

    # Query methods:

    @abc.abstractmethod
    def execute(self, sql_query: str, **params) -> None:
        """Execute a query against the underlying client."""
        ...

    @abc.abstractmethod
    def query(self, sql_query: str, **params) -> Iterable:
        """Perform the query and return an iterable of the results."""
        ...


DbFactory = Callable[..., Db]


class DbRegistry(URLHandlerRegistry[DbFactory]):
    """Registry for databases."""

    def get_handler(self, scheme: str) -> DbFactory:
        """Get the handler for the give scheme. Raise an URLError if no handler is registred."""
        if scheme not in self.registry:
            logger.info("trying to return the default SQLA client")
            return clients.SQLAlchemyClient

        return self.registry[scheme]


class _DbRegistryHolder:
    """Module level singleton."""

    instance: ClassVar[DbRegistry] = DbRegistry()


DB_REGISTRY = _DbRegistryHolder().instance
