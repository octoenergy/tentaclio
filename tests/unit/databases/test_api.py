import pytest

from tentaclio.clients import base_client
from tentaclio.credentials import load_credentials_injector
from tentaclio.credentials.env import add_credentials_from_env
from tentaclio.databases import api
from tentaclio.databases.db_registry import DB_REGISTRY


class FakeDb(base_client.BaseClient["FakeDb"]):
    """docstring for ClassName"""

    def __init__(self, url):
        self.url = url

    def _connect(self) -> "FakeDb":
        return self


@pytest.fixture(scope="session")
def fake_db():
    add_credentials_from_env(
        load_credentials_injector(),
        {"TENTACLIO__CONN__TEST": "dbtest://costantine:tentacl3@mytest/"},
    )
    DB_REGISTRY.register("dbtest", FakeDb)


def test_database(mocker):
    mocked_client = mocker.patch("tentaclio.clients.SQLAlchemyClient")
    api.db("sqlite://hostname/database")
    mocked_client.assert_called_once()


def test_authenticate_db(fake_db):
    my_db = api.db("dbtest://mytest/")
    url = my_db.url

    assert url.username == "costantine"
    assert url.password == "tentacl3"
