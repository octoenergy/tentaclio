import functools
import logging

from .env import add_creds_from_env
from .injection import CredentialsInjector
from .reader import add_credentials_from_reader, get_secrets_file


logger = logging.getLogger(__name__)


__all__ = ["load_credentials_injector"]


def _load_from_file(injector: CredentialsInjector, path: str) -> CredentialsInjector:
    try:
        with open(path, "r") as f:
            return add_credentials_from_reader(injector, f)
    except IOError as e:
        logger.error("Error while loading secrets file {path}")
        logger.exception(e)
        return injector


@functools.lru_cache(maxsize=1, typed=True)
def load_credentials_injector() -> CredentialsInjector:
    """Load the credentials injector fetching configuration from the environment."""
    injector = CredentialsInjector()
    injector = add_creds_from_env(injector)
    secrets_file = get_secrets_file()
    if secrets_file != "":
        _load_from_file(injector, secrets_file)
    return injector
