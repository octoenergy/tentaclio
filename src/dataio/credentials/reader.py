import io
import os
import logging

import yaml

from dataio import protocols, urls
from dataio.credentials import injection


logger = logging.getLogger(__name__)

SECRETS = "secrets"
OCTOIO_SECRETS_FILE = "OCTOIO__SECRETS_FILE"


def _load_creds_from_yaml(yaml_reader: protocols.Reader) -> dict:
    loaded_data = yaml.load(io.StringIO(yaml_reader.read()))
    if SECRETS not in loaded_data:
        raise KeyError(f"no secrets in yaml data. Make sure the file has a '{SECRETS}' element")
    return loaded_data[SECRETS]


def get_secrets_file() -> str:
    return os.getenv(OCTOIO_SECRETS_FILE, "")


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
            logger.exception(e)

    return injector
