from dataio.buffers import DatabaseCsvWriter
from dataio.clients import postgres_client
from dataio.protocols import ReaderClosable, WriterClosable
from dataio.urls import URL


__all__ = ["PostgresURLHandler"]


def _get_table(url: URL) -> str:
    parts = url.path.split("::")
    if len(parts) != 2 or parts[1] == "":
        raise ValueError("No table provided for dumping the csv data")
    table = parts[1]
    return table


class PostgresURLHandler:
    """Handler for opening writers and readers ."""

    def open_reader_for(self, url: URL, mode: str, extras: dict) -> ReaderClosable:
        """Not implemented."""
        raise NotImplementedError("Reading csv from postgres not implemented")

    def open_writer_for(self, url: URL, mode: str, extras: dict) -> WriterClosable:
        """Open writer to dump the csv into a table in database.

           This stream writer only works for csv data at the moment. The url must contain
           a table as the last part of the path separated by a doble colon i.e.
           `postgresql://user:pass@host/database::table`. The target table needs the columns
           with the same names as the field names.
        """
        table = _get_table(url)
        client = postgres_client.PostgresClient(url, **extras)
        return DatabaseCsvWriter(client, table)
