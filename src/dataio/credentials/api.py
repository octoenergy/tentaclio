import functools

from .env import add_creds_from_env
from .injection import CredentialsInjector

__all__ = ["load_credentials_injector"]


@functools.lru_cache(maxsize=1, typed=True)
def load_credentials_injector() -> CredentialsInjector:
    """Load the credentials injector fetching configuration from the environment."""
    injector = CredentialsInjector()
    injector = add_creds_from_env(injector)
    return injector
