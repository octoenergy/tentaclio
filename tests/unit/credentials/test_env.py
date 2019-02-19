from dataio import urls
from dataio.credentials.env import add_credentials_from_env
from dataio.credentials.injection import CredentialsInjector

import pytest


def test_add_credentials(register_handler):
    env = {
        "one_var": "registered://user:pass@hostname",
        "OCTOIO__CONN__DB": "registered://mydb/database",
    }
    injector = add_credentials_from_env(CredentialsInjector(), env)
    assert len(injector.registry["registered"]) == 1
    assert injector.registry["registered"][0] == urls.URL("registered://mydb/database")


def test_add_credentials_bad_url(register_handler):
    env = {"OCTOIO__CONN__DB": None}
    with pytest.raises(Exception):
        add_credentials_from_env(CredentialsInjector(), env)
