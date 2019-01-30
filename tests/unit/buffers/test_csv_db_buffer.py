import csv
import io

import pytest

from dataio.buffers import DatabaseCsvWriter


@pytest.fixture
def csv_data():
    cols = [1, 2]
    data_dict = {f"col_{i}": f"val{i}" for i in cols}
    data = io.StringIO("")
    csv_writer = csv.DictWriter(data, list(data_dict.keys()))
    csv_writer.writeheader()
    csv_writer.writerow(data_dict)
    data_dict = {f"col_{i}": f"moar_{i}" for i in cols}
    csv_writer.writerow(data_dict)

    data.seek(0)
    return data


def test_dump_csv(csv_data, csv_dumper):

    with DatabaseCsvWriter(csv_dumper, "my_table") as writer:
        writer.write(csv_data.getvalue())

    # reset the reader buffer
    csv_data.seek(0)
    assert set(csv_dumper.columns) == set(["col_1", "col_2"])
    assert csv_dumper.dest_table == "my_table"
    assert csv_dumper.buff.getvalue() == csv_data.getvalue()
