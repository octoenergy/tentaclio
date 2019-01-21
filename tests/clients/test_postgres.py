import pandas as pd
import pytest
from dataio.clients import exceptions, postgres_client

TEST_TABLE_NAME = "dump_test"
TABLE_COLUMNS = ["column_int", "column_str", "column_float"]


@pytest.fixture()
def test_db(db_client):
    sql_query = f"""
    CREATE TABLE {TEST_TABLE_NAME} (
      {TABLE_COLUMNS[0]} INTEGER,
      {TABLE_COLUMNS[1]} VARCHAR(256),
      {TABLE_COLUMNS[2]} NUMERIC(8, 3)
    );
    """
    db_client.execute(sql_query)
    yield db_client
    db_client.execute(f"DROP TABLE {TEST_TABLE_NAME}")


@pytest.fixture()
def test_df():
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
    def test_postgres_url(self, url, username, password, hostname, port, path):
        parsed_url = postgres_client.PostgresClient(url).url

        assert parsed_url.scheme == "postgresql"
        assert parsed_url.hostname == hostname
        assert parsed_url.username == username
        assert parsed_url.password == password
        assert parsed_url.port == port
        assert parsed_url.path == path

    def test_dump_then_get_df(self, test_db, test_df):
        test_db.dump_df(test_df, TEST_TABLE_NAME)
        retrieved_df = test_db.get_df(f"SELECT * FROM {TEST_TABLE_NAME}")

        assert retrieved_df.equals(test_df)
