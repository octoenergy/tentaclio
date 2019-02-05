import csv
import io
import logging
from typing import Any, Sequence

from dataio.protocols import AnyReaderWriter, Reader
from typing_extensions import Protocol

from .base_buffer import BaseBuffer

logger = logging.getLogger(__name__)

__all__ = ["DatabaseCsvWriter"]


def _get_field_names(reader: io.StringIO) -> Sequence[str]:
    """Get the field names from the contents."""

    csv_reader = csv.DictReader(reader)
    next(csv_reader)
    reader.seek(0)
    return csv_reader.fieldnames


class CsvDumper(Protocol):
    """Csv into db dumping contract."""

    def __enter__(self) -> Any:
        ...

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        ...

    def dump_csv(
        self, csv_reader: Reader, columns: Sequence[str], dest_table: str
    ) -> None:
        ...


class DatabaseCsvWriter(BaseBuffer):
    """Writer that dumps the csv formated data into the specified table.

    The connection is done through the relevant client.
    The csv data is expected to be native's python dialect. namely first row defines the header and
    the data is delimited by `,`.
    """

    def __init__(self, csv_dumper: CsvDumper, table: str) -> None:
        """Create a new csv writer initilising the internal buffer.

        :csv_dumper: an object that is able to write csv files into a table.
        :table: the name of the destination table in the database.
        """
        self.csv_dumper = csv_dumper
        self.table = table
        self.buff = io.StringIO()

    def write(self, contents) -> int:
        """Append contents to the internal buffer."""
        return self.buff.write(contents)

    def _flush(self) -> None:
        """Use the client to dump the data into the configured table."""
        self.buff.seek(0)
        field_names = _get_field_names(self.buff)
        with self.csv_dumper:
            self.csv_dumper.dump_csv(self.buff, field_names, self.table)

    def close(self) -> None:
        """Close the internal buffer and flush the contents to db."""
        self._flush()
        self.buff.close()

    # base_buffer overwrites
    def __enter__(self) -> AnyReaderWriter:
        """Return self as Writer."""
        # Note a pr down the line won't force this method to return a Any, but a writer
        return self

    def __exit__(self, *args) -> None:
        """Close the writer."""
        self.close()
