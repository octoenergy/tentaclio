"""Postgres query client.

Adds the convinience methods for loading csv data using copy_expert,
which is more performant than using sql alchemy functions.
"""

import io
from typing import Sequence

import pandas as pd

from tentaclio import protocols

from . import decorators, sqla_client


__all__ = ["PostgresClient"]


class PostgresClient(sqla_client.SQLAlchemyClient):
    """Postgres client, backed by a SQLAlchemy connection."""

    allowed_schemes = ["postgresql"]

    # Postgres Copy Expert methods:

    @decorators.check_conn
    def dump_df(self, df: pd.DataFrame, dest_table: str) -> None:
        """Dump a data frame into an existing Postgres table."""
        buff = io.StringIO()
        df.to_csv(buff, index=False)
        buff.seek(0)
        self._copy_expert_csv(buff, df.columns, dest_table)

    @decorators.check_conn
    def dump_csv(
        self, csv_reader: protocols.Reader, columns: Sequence[str], dest_table: str
    ) -> None:
        """Dump a csv reader into the database."""
        self._copy_expert_csv(csv_reader, columns, dest_table)

    def _copy_expert_csv(
        self, csv_reader: protocols.Reader, columns: Sequence[str], dest_table: str
    ) -> None:
        """Dump a csv reader into the given table."""
        sql_columns = ",".join(columns)
        sql_query = f"""COPY {dest_table} ({sql_columns}) FROM STDIN
                        WITH CSV HEADER DELIMITER AS ','
                        NULL AS 'NULL';"""
        raw_conn = self.conn.engine.raw_connection()
        try:
            raw_conn.cursor().copy_expert(sql_query, csv_reader)
        except Exception:
            raw_conn.rollback()
            raise
        else:
            raw_conn.commit()
