from dataio import urls
from dataio.credentials.env import add_creds_from_env
from dataio.credentials.injection import CredentialsInjector


def test_add_credentials(register_handler):
    env = {
        "one_var": "registered://user:pass@hostname",
        "OCTOIO_CONN_DB": "registered://mydb/database",
    }
    injector = add_creds_from_env(CredentialsInjector(), env)
    assert len(injector.registry["registered"]) == 1
    assert injector.registry["registered"][0] == urls.URL("registered://mydb/database")


def test_add_credentials_bad_url(register_handler):
    env = {"OCTOIO_CONN_DB": None}
    injector = add_creds_from_env(CredentialsInjector(), env)
    assert len(injector.registry["registered"]) == 0
