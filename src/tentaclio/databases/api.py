"""Tentaclios api to deal with database clients."""
from tentaclio import credentials

from .db_registry import DB_REGISTRY, Db


__all__ = ["db"]


def db(url: str, **kwargs) -> Db:
    """Create an authenticated db client."""
    authenticated = credentials.authenticate(url)
    factory = DB_REGISTRY.get_handler(authenticated.scheme)
    return factory(authenticated, **kwargs)
