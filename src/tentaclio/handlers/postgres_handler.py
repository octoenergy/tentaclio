"""Handler for postgresql:// urls."""
from typing import Tuple

from tentaclio.clients import postgres_client
from tentaclio.protocols import ReaderClosable, WriterClosable
from tentaclio.streams import csv_db_stream
from tentaclio.urls import URL


__all__ = ["PostgresURLHandler"]


def _split_url(url: URL) -> Tuple[URL, str]:
    parts = url.path.split("::")
    if len(parts) != 2 or parts[1] == "":
        raise ValueError("No table provided for dumping the csv data")
    path = parts[0]
    table = parts[1]
    conn_url = URL.from_components(
        scheme=url.scheme,
        username=url.username,
        password=url.password,
        hostname=url.hostname,
        port=url.port,
        path=path,
        query=url.query,
    )
    return conn_url, table


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
        conn_url, table = _split_url(url)
        client = postgres_client.PostgresClient(conn_url, **extras)
        return csv_db_stream.DatabaseCsvWriter(client, table)
