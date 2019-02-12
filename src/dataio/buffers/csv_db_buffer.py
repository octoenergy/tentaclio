import csv
import io
import logging
from typing import Any, Sequence

from typing_extensions import Protocol

from dataio import protocols


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

    # The context manager is an actual protcol defined in the
    # std library .... but it can't  be inherited from
    # Also protocols and return types still have gotchas,
    # here PostgresClient won't be recognised as a CsvDumper
    # the reason why we're using any
    # def __enter__(self) -> "CsvDumper":
    def __enter__(self) -> Any:
        ...

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        ...

    def dump_csv(
        self, csv_reader: protocols.Reader, columns: Sequence[str], dest_table: str
    ) -> None:
        ...


class DatabaseCsvWriter:
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

    def __enter__(self) -> protocols.Writer:
        """Return self as Writer."""
        return self

    def __exit__(self, *args) -> None:
        """Close the writer."""
        self.close()
