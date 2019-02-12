import functools
import logging

from .env import add_credentials_from_env
from .injection import CredentialsInjector
from .reader import add_credentials_from_env_file


logger = logging.getLogger(__name__)


__all__ = ["load_credentials_injector"]


#  Singleton
@functools.lru_cache(maxsize=1, typed=True)
def load_credentials_injector() -> CredentialsInjector:
    """Load the credentials injector fetching configuration from the environment."""
    injector = CredentialsInjector()
    injector = add_credentials_from_env(injector)
    injector = add_credentials_from_env_file(injector)
    return injector
