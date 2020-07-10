"""Postgres query client.

Adds the convinience methods for loading csv data using copy_expert,
which is more performant than using sql alchemy functions.
"""

import io
import os
from typing import Sequence

import pandas as pd

from tentaclio import protocols

from . import decorators, sqla_client


__all__ = ["PostgresClient"]


class PostgresClient(sqla_client.SQLAlchemyClient):
    """Postgres client, backed by a SQLAlchemy connection.

    While connecting will check TENTACLIO__PG_APPLICATION_NAME to set
    the applicationName in the connection string.
    """

    TENTACLIO__PG_APPLICATION_NAME = "TENTACLIO__PG_APPLICATION_NAME"

    allowed_schemes = ["postgresql"]

    def _extract_url_params(self):
        super()._extract_url_params()
        application_name = os.getenv(self.TENTACLIO__PG_APPLICATION_NAME, "")
        if application_name != "":
            if self.url_query is None:
                self.url_query = {}
            # overrides the value that might be set in the tentaclio file
            self.url_query["application_name"] = application_name

    # Postgres Copy Expert methods:
    @decorators.check_conn
    def get_df_unsafe(self, sql_query: str, params: dict = None, **kwargs) -> pd.DataFrame:
        """Run a raw SQL query and return a data framem using COPY.

        Params:
            sql_query: query to execute
            params: not supported; must be None
            **kwargs: additional kwargs to pass to `pandas.read_csv`
        """
        if params:
            raise NotImplementedError(
                "Support for `params` is not implemented; please provide a pre-formatted query."
            )
        copy_sql = f"COPY ({sql_query.rstrip(';')}) TO STDOUT WITH CSV HEADER"
        buffer = io.StringIO()
        raw_conn = self._get_raw_conn()
        raw_conn.cursor().copy_expert(copy_sql, buffer)
        buffer.seek(0)
        df = pd.read_csv(buffer, **kwargs)
        return df

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
        raw_conn = self._get_raw_conn()
        try:
            raw_conn.cursor().copy_expert(sql_query, csv_reader)
        except Exception:
            raw_conn.rollback()
            raise
        else:
            raw_conn.commit()
