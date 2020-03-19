import pandas as pd
import pytest
import sqlalchemy as sqla


TEST_TABLE_NAME = "test_table"
TEST_COLUMNS = ["column_int", "column_str", "column_float"]
TEST_VALUES = [1, "test_1", 123.456]


@pytest.fixture
def fixture_client(db_client):
    test_meta = sqla.MetaData()
    sqla.Table(
        # Meta
        TEST_TABLE_NAME,
        test_meta,
        # Columns
        sqla.Column(TEST_COLUMNS[0], sqla.Integer),
        sqla.Column(TEST_COLUMNS[1], sqla.String(256)),
        sqla.Column(TEST_COLUMNS[2], sqla.Float),
    )
    db_client.set_schema(test_meta)
    yield db_client
    db_client.delete_schema(test_meta)


@pytest.fixture
def fixture_df():
    df = pd.DataFrame(data=[TEST_VALUES], columns=TEST_COLUMNS)
    return df


class TestPostgresClient:
    def test_executing_and_querying_sql(self, fixture_client):
        sql_insert = f"""INSERT INTO {TEST_TABLE_NAME} VALUES
                         ({TEST_VALUES[0]}, '{TEST_VALUES[1]}', {TEST_VALUES[2]});"""
        sql_query = f"SELECT * FROM {TEST_TABLE_NAME};"

        fixture_client.execute(sql_insert)
        result = fixture_client.query(sql_query)

        assert list(result.fetchone()) == TEST_VALUES

    def test_dumping_and_getting_df(self, fixture_client, fixture_df):
        fixture_client.dump_df(fixture_df, TEST_TABLE_NAME)
        retrieved_df = fixture_client.get_df(f"SELECT * FROM {TEST_TABLE_NAME};")

        assert retrieved_df.equals(fixture_df)

    def test_dumping_and_getting_df_unsafe(self, fixture_client, fixture_df):
        fixture_client.dump_df(fixture_df, TEST_TABLE_NAME)
        retrieved_df = fixture_client.get_df_unsafe(f"SELECT * FROM {TEST_TABLE_NAME};")

        assert retrieved_df.equals(fixture_df)
