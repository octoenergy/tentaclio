"""Load credentials from the environment."""
import logging
import os
from typing import Dict, List, Optional, cast

from tentaclio import urls

from .injection import CredentialsInjector


logger = logging.getLogger(__name__)


Env = Dict[str, str]

TENTACLIO_CONN_PREFIX = "TENTACLIO__CONN__"


def _get_connection_urls(env: Env) -> List[urls.URL]:
    connections = []
    for key, val in env.items():
        if key.startswith(TENTACLIO_CONN_PREFIX):
            try:
                connections.append(urls.URL(val))
            except Exception as e:
                logger.error(f"Error parsing credentials from the environment variable {key}")
                raise e
    return connections


def add_credentials_from_env(
    injector: CredentialsInjector, env: Optional[Env] = None
) -> CredentialsInjector:
    """Add urls with credentials from the environment.

    this funciton will scan the environment and add the variables
    with the prefix TENTACLIO_CONN__ to the pool of
    available connection strings.
    """
    if env is None:
        env = cast(Env, os.environ)
    urls = _get_connection_urls(env)
    for url in urls:
        injector.register_credentials(url)
    return injector
