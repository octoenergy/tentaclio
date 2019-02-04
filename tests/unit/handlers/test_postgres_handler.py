import csv
import io

from dataio import open_writer

import pytest


@pytest.fixture
def csv_data():
    cols = [1, 2]
    data_dict = {f"col_{i}": f"val{i}" for i in cols}
    data = io.StringIO("")
    csv_writer = csv.DictWriter(data, list(data_dict.keys()))
    csv_writer.writeheader()
    csv_writer.writerow(data_dict)

    data.seek(0)
    return data


def test_dump_csv(csv_data, csv_dumper, mocker):
    mock = mocker.patch("dataio.clients.postgres_client.PostgresClient")
    recorder = csv_dumper
    mock.return_value = recorder

    with open_writer("postgresql://localhost/database::my_table") as writer:
        writer.write(csv_data.read())

    csv_data.seek(0)
    assert set(recorder.columns) == set(["col_1", "col_2"])
    assert recorder.dest_table == "my_table"
    assert recorder.buff.getvalue() == csv_data.getvalue()


def test_dump_csv_no_table(csv_data, csv_dumper, mocker):
    mock = mocker.patch("dataio.clients.postgres_client.PostgresClient")
    recorder = csv_dumper
    mock.return_value = recorder
    with pytest.raises(ValueError):
        with open_writer("postgresql://localhost/database") as writer:
            writer.write(csv_data.read())
