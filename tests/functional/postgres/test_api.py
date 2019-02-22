import pandas as pd
import pytest
import sqlalchemy as sqla

import dataio
from dataio import clients, credentials


TEST_TABLE_NAME = "test_table"
TEST_COLUMNS = ["col_1", "col_2", "col_3"]
TEST_VALUES = [0, 1, 2]


@pytest.fixture
def fixture_client(db_client):
    test_meta = sqla.MetaData()
    sqla.Table(
        # Meta
        TEST_TABLE_NAME,
        test_meta,
        # Columns
        sqla.Column(TEST_COLUMNS[0], sqla.Integer),
        sqla.Column(TEST_COLUMNS[1], sqla.Integer),
        sqla.Column(TEST_COLUMNS[2], sqla.Integer),
    )
    db_client.set_schema(test_meta)
    yield db_client
    db_client.delete_schema(test_meta)


@pytest.fixture
def fixture_df():
    df = pd.DataFrame(data=[TEST_VALUES], columns=TEST_COLUMNS)
    return df


def test_authenticated_api_calls(fixture_client, fixture_df):
    with dataio.open(f"postgresql://localhost/dataio-test::{TEST_TABLE_NAME}", mode="w") as writer:
        fixture_df.to_csv(writer, index=False)

    with clients.PostgresClient(
        credentials.authenticate("postgresql://localhost/dataio-test")
    ) as client:
        retrieved_df = client.get_df(f"select * from {TEST_TABLE_NAME}")

    assert retrieved_df.equals(fixture_df)
