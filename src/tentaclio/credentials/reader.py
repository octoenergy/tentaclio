"""Read credentials from yaml file."""
import io
import logging
import os

import yaml

from tentaclio import protocols, urls
from tentaclio.credentials import injection


logger = logging.getLogger(__name__)

SECRETS = "secrets"
TENTACLIO_SECRETS_FILE = "TENTACLIO__SECRETS_FILE"


def add_credentials_from_env_file(
    injector: injection.CredentialsInjector,
) -> injection.CredentialsInjector:
    """Add credentials from a configurable yaml file.

    The path of this file is expected to be set in the environment as
    `TENTACLIO__SECRETS_FILE`
    """
    secrets_file = os.getenv(TENTACLIO_SECRETS_FILE, "")
    if secrets_file != "":
        injector = _load_from_file(injector, secrets_file)
    return injector


def _load_creds_from_yaml(yaml_reader: protocols.Reader) -> dict:
    loaded_data = yaml.safe_load(io.StringIO(yaml_reader.read()))
    if SECRETS not in loaded_data:
        raise KeyError(f"no secrets in yaml data. Make sure the file has a '{SECRETS}' element")
    return loaded_data[SECRETS]


def _load_from_file(
    injector: injection.CredentialsInjector, path: str
) -> injection.CredentialsInjector:
    try:
        with open(path, "r") as f:
            return add_credentials_from_reader(injector, f)
    except IOError as e:
        logger.error("Error while loading secrets file {path}")
        raise e


def add_credentials_from_reader(
    injector: injection.CredentialsInjector, yaml_reader: protocols.Reader
) -> injection.CredentialsInjector:
    """Read the credentials from a yml.

    The file has the follwing format:
        secrets:
            my_creds_name: http://user:password@google.com/path
            my_db: postgres://user_db:password@octoenergy.com/databasek

    """
    creds = _load_creds_from_yaml(yaml_reader)
    for name, url in creds.items():
        logger.info(f"Adding secret: {name}")
        try:
            injector.register_credentials(urls.URL(url))
        except Exception as e:
            logger.error(f"Error while registering credentials {name}:{url} from file")
            raise e

    return injector
