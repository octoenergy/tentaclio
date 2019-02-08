import pytest

from dataio.clients.sqla_client import SQLAlchemyClient


@pytest.mark.parametrize(
    "url, drivername, username, password, hostname, port, database",
    [
        (
            "postgresql://login:pass@localhost",
            "postgresql",
            "login",
            "pass",
            "localhost",
            None,
            "",
        ),
        (
            "awsathena+rest://:@localhost:5432/database",
            "awsathena+rest",
            "",
            "",
            "localhost",
            5432,
            "database",
        ),
    ],
)
def test_parsing_postgres_url(url, drivername, username, password, hostname, port, database):
    client = SQLAlchemyClient(url)

    assert client.drivername == drivername
    assert client.host == hostname
    assert client.username == username
    assert client.password == password
    assert client.port == port
    assert client.database == database


def test_execute_query(sqlite_url):
    client = SQLAlchemyClient(sqlite_url)
    with client:
        client.execute(
            """
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
            """
        )
        client.execute("""INSERT INTO test_table VALUES (0, 'Eric'), (1, 'Igor')""")

    with client:
        df = client.get_df("test_table")

    assert df["id"].values.tolist() == [0, 1]
    assert df["name"].values.tolist() == ["Eric", "Igor"]
