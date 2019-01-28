import csv
import io
import logging
from typing import List

from dataio import AnyReaderWriter, Reader
from typing_extensions import Protocol

from .base_buffer import BaseBuffer

logger = logging.getLogger(__name__)

__all__ = ["DatabaseCsvWriter"]


def _get_field_names(reader: io.StringIO):
    """ gets the field names from the contents """
    csv_reader = csv.DictReader(reader)
    next(csv_reader)
    reader.seek(0)
    return csv_reader.fieldnames


class CsvDumper(Protocol):
    """ Csv into db dumping contract """

    def dump_csv(self, csv_reader: Reader, columns: List[str], dest_table: str) -> None:
        pass


class DatabaseCsvWriter(BaseBuffer):
    """ Writer that dumps the csv formated data into the specified table.
    The connection is done through the relevant client """

    def __init__(self, csv_dumper: CsvDumper, table: str) -> None:
        """
        :csv_dumper: an object that is able to write csv files into a table
        :table: the name of the destination table in the database
        """
        self.csv_dumper = csv_dumper
        self.table = table
        self.buff = io.StringIO()

    def write(self, contents) -> int:
        """ append contents to the internal buffer """
        return self.buff.write(contents)

    def _flush(self) -> None:
        """uses the client to dump the data into the configured table """
        self.buff.seek(0)
        field_names = _get_field_names(self.buff)
        self.csv_dumper.dump_csv(self.buff, field_names, self.table)

    def close(self) -> None:
        self._flush()
        self.buff.close()

    # base_buffer overwrites
    def __enter__(self) -> AnyReaderWriter:
        return self

    def __exit__(self, *args) -> None:
        self.close()
