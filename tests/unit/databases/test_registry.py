from tentaclio.clients import SQLAlchemyClient
from tentaclio.databases import db_registry


def test_default_database(mocker):
    reg = db_registry.DbRegistry()
    db = reg.get_handler("ijustmadeupthisscheme")
    assert db == SQLAlchemyClient
