import csv
import io
from typing import List

from dataio import Reader
from dataio.buffers import DatabaseCsvWriter

import pytest


class CsvDumperRecorder:

    def dump_csv(self, csv_reader: Reader, columns: List[str], dest_table: str) -> None:
        self.buff = io.StringIO()
        self.buff.write(csv_reader.read())
        self.buff.seek(0)
        self.columns = columns
        self.dest_table = dest_table


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


def test_dump_csv(csv_data):
    recorder = CsvDumperRecorder()

    with DatabaseCsvWriter(recorder, "my_table") as writer:
        writer.write(csv_data.getvalue())

    # reset the reader buffer
    csv_data.seek(0)
    assert set(recorder.columns) == set(["col_1", "col_2"])
    assert recorder.dest_table == "my_table"
    assert recorder.buff.getvalue() == csv_data.getvalue()
