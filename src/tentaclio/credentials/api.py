"""Public module functions."""
import functools
import logging

from tentaclio import urls

from .env import add_credentials_from_env
from .injection import CredentialsInjector
from .reader import add_credentials_from_env_file


logger = logging.getLogger(__name__)


__all__ = ["load_credentials_injector", "authenticate"]


#  Singleton
@functools.lru_cache(maxsize=1, typed=True)
def load_credentials_injector() -> CredentialsInjector:
    """Load the credentials injector fetching configuration from the environment."""
    injector = CredentialsInjector()
    injector = add_credentials_from_env(injector)
    injector = add_credentials_from_env_file(injector)
    return injector


def authenticate(url: str) -> urls.URL:
    """Authenticate url."""
    return load_credentials_injector().inject(urls.URL(url))
