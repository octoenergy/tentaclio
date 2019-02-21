import csv
import io

import pandas as pd
import pytest
import sqlalchemy as sqla

import dataio


TEST_CSV_TABLE_NAME = "csv_table"
TEST_COLUMNS = ["col_1", "col_2", "col_3"]
TEST_VALUES = [0, 1, 2]


@pytest.fixture()
def csv_client(db_client):
    test_meta = sqla.MetaData()
    sqla.Table(
        # Meta
        TEST_CSV_TABLE_NAME,
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
def csv_data():
    data_dict = {key: value for key, value in zip(TEST_COLUMNS, TEST_VALUES)}
    data = io.StringIO("")
    csv_writer = csv.DictWriter(data, list(data_dict.keys()))
    csv_writer.writeheader()
    csv_writer.writerow(data_dict)

    data.seek(0)
    return data


class TestCSVWriter:
    def test_write_csv(self, postgres_url, csv_client, csv_data):
        df = pd.read_csv(csv_data)
        with dataio.open(postgres_url + "::csv_table", mode="w") as writer:
            df.to_csv(writer, index=False)

        result = csv_client.query(f"select * from {TEST_CSV_TABLE_NAME}")
        assert list(result.fetchone()) == TEST_VALUES
