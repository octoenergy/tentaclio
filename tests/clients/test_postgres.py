import pandas as pd
import pytest
import sqlalchemy as sqla
from dataio.clients import exceptions, postgres_client

TEST_TABLE_NAME = "dump_test"
TABLE_COLUMNS = ["column_int", "column_str", "column_float"]


@pytest.fixture()
def fixture_client(db_client):
    test_meta = sqla.MetaData()
    sqla.Table(
        # Meta
        TEST_TABLE_NAME, test_meta,
        # Columns
        sqla.Column(TABLE_COLUMNS[0], sqla.Integer),
        sqla.Column(TABLE_COLUMNS[1], sqla.String(256)),
        sqla.Column(TABLE_COLUMNS[2], sqla.Float),
    )
    db_client.set_schema(test_meta)
    yield db_client
    db_client.delete_schema(test_meta)


@pytest.fixture()
def fixture_df():
    data = [[1, "test_1", 123.456], [2, "test_2", 456.789]]
    df = pd.DataFrame(data=data, columns=TABLE_COLUMNS)
    return df


class TestPostgresClient:
    @pytest.mark.parametrize(
        "url", ["file:///test.file", "ftp://:@localhost", "s3://:@s3"]
    )
    def test_invalid_scheme(self, url):
        with pytest.raises(exceptions.PostgresError):
            postgres_client.PostgresClient(url)

    @pytest.mark.parametrize(
        "url,username,password,hostname,port,path",
        [
            ("postgresql://:@localhost", "", "", "localhost", None, ""),
            ("postgresql://login:pass@localhost", "login", "pass", "localhost", None, ""),
            ("postgresql://:@localhost:5432", "", "", "localhost", 5432, ""),
            ("postgresql://:@localhost:5432/database", "", "", "localhost", 5432, "database"),
        ],
    )
    def test_parsing_postgres_url(self, url, username, password, hostname, port, path):
        parsed_url = postgres_client.PostgresClient(url).url

        assert parsed_url.scheme == "postgresql"
        assert parsed_url.hostname == hostname
        assert parsed_url.username == username
        assert parsed_url.password == password
        assert parsed_url.port == port
        assert parsed_url.path == path

    def test_dumping_and_getting_df(self, fixture_client, fixture_df):
        fixture_client.dump_df(fixture_df, TEST_TABLE_NAME)
        retrieved_df = fixture_client.get_df(f"SELECT * FROM {TEST_TABLE_NAME}")

        assert retrieved_df.equals(fixture_df)
