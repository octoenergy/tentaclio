import pandas as pd
import pytest


TEST_TABLE_NAME = "dump_test"
TABLE_COLUMNS = ["column_int", "column_str", "column_float"]


@pytest.yield_fixture()
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
    data = [
        [1, "test_1", 123.456],
        [2, "test_2", 456.789],
    ]
    df = pd.DataFrame(data=data, columns=TABLE_COLUMNS)
    return df


class TestPostgresClient:
    def test_dump_then_get_df(self, test_db, test_df):
        test_db.dump_df(test_df, TEST_TABLE_NAME)
        retrieved_df = test_db.get_df(f"SELECT * FROM {TEST_TABLE_NAME}")

        assert retrieved_df.equals(test_df)
