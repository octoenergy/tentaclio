import pytest

from dataio.clients.sqla_client import SQLAlchemyClient


@pytest.mark.parametrize(
    "url, drivername, username, password, hostname, port, database, query",
    [
        (
            "postgresql://login:pass@localhost",
            "postgresql",
            "login",
            "pass",
            "localhost",
            None,
            "",
            None,
        ),
        (
            "awsathena+rest://:@localhost:5432/database?key=value",
            "awsathena+rest",
            "",
            "",
            "localhost",
            5432,
            "database",
            dict(key="value"),
        ),
    ],
)
def test_parsing_postgres_url(
    url, drivername, username, password, hostname, port, database, query
):
    client = SQLAlchemyClient(url)

    assert client.drivername == drivername
    assert client.host == hostname
    assert client.username == username
    assert client.password == password
    assert client.port == port
    assert client.database == database
    assert client.url_query == query


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
        client.execute("""INSERT INTO test_table VALUES (0, 'Javi'), (1, 'Eric'), (2, 'Igor')""")

    with client:
        df = client.get_df("test_table")
    assert df["id"].values.tolist() == [0, 1, 2]
    assert df["name"].values.tolist() == ["Javi", "Eric", "Igor"]
