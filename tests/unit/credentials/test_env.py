import pytest

from tentaclio import urls
from tentaclio.credentials.env import add_credentials_from_env
from tentaclio.credentials.injection import CredentialsInjector


def test_add_credentials():
    env = {
        "one_var": "scheme://user:pass@hostname",
        "TENTACLIO__CONN__DB": "scheme://mydb/database",
    }
    injector = add_credentials_from_env(CredentialsInjector(), env)
    assert len(injector.registry["scheme"]) == 1
    assert injector.registry["scheme"][0] == urls.URL("scheme://mydb/database")


def test_add_credentials_bad_url():
    env = {"TENTACLIO__CONN__DB": None}
    with pytest.raises(Exception):
        add_credentials_from_env(CredentialsInjector(), env)
