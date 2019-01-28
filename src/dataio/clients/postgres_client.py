import contextlib
import io
from typing import Generator, List, Optional

import pandas as pd
from dataio.protocols import Reader
from sqlalchemy.engine import Connection, create_engine, result
from sqlalchemy.engine import url as sqla_url
from sqlalchemy.orm import session, sessionmaker
from sqlalchemy.sql.schema import MetaData

from . import base_client, decorators, exceptions

__all__ = ["PostgresClient", "bound_session", "atomic_session"]


SessionGenerator = Generator[None, session.Session, None]


class PostgresClient(base_client.QueryClient):
    """
    Generic Postgres hook, backed by a SQLAlchemy connection
    """

    conn: Optional[Connection]
    engine = None
    execution_options: dict
    connect_args: dict

    def __init__(
        self, url: str, execution_options: dict = None, connect_args: dict = None
    ) -> None:
        self.execution_options = execution_options or {}
        self.connect_args = connect_args or {}
        super().__init__(url)

        if self.url.scheme != "postgresql":
            raise exceptions.PostgresError(f"Incorrect scheme {self.url.scheme}")

        # Exception: database not a path
        if self.url.path != "":
            self.url.path = self.url.path[1:]

    # Connection methods:

    def connect(self) -> Connection:
        parsed_url = sqla_url.URL(
            drivername=self.url.scheme,
            username=self.url.username,
            password=self.url.password,
            host=self.url.hostname,
            port=self.url.port,
            database=self.url.path,
        )
        if self.engine is None:
            self.engine = create_engine(
                parsed_url,
                execution_options=self.execution_options,
                connect_args=self.connect_args
            )
        return self.engine.connect()

    # Schema methods:

    def set_schema(self, meta_data: MetaData) -> None:
        meta_data.create_all(bind=self.conn)

    def delete_schema(self, meta_data: MetaData) -> None:
        meta_data.drop_all(bind=self.conn)

    # Query methods:

    @decorators.check_conn()
    def query(self, sql_query: str, **params) -> result.ResultProxy:
        """
        Execute a read-only SQL query, and return results

        Remark: will NOT commit any changes to DB
        """
        return self.conn.execute(sql_query, params=params)  # type: ignore

    @decorators.check_conn()
    def execute(self, sql_query: str, **params) -> None:
        """
        Execute a raw SQL query command

        Remark: will commit changes to DB
        """
        trans = self.conn.begin()  # type: ignore
        try:
            self.conn.execute(sql_query, params=params)  # type: ignore
        except Exception:
            trans.rollback()
            raise
        else:
            trans.commit()

    # Dataframe methods:

    @decorators.check_conn()
    def get_df(self, sql_query: str, params: dict = None, **kwargs) -> pd.DataFrame:
        """
        Run a raw SQL query and return a data frame
        """
        df = pd.read_sql(sql_query, self.conn, params=params, **kwargs)
        if df.empty:
            raise exceptions.PostgresError("Empty Pandas dataframe content")
        return df

    @decorators.check_conn()
    def dump_df(self, df: pd.DataFrame, dest_table: str) -> None:
        """Dump a data frame into an existing Postgres table."""
        buff = io.StringIO()
        df.to_csv(buff, index=False)
        buff.seek(0)
        self._copy_expert_csv(buff, df.columns, dest_table)

    @decorators.check_conn()
    def dump_csv(self, csv_reader: Reader, columns: List[str], dest_table: str) -> None:
        """Dump a csv reader into the database."""
        self._copy_expert_csv(csv_reader, columns, dest_table)

    def _copy_expert_csv(self, csv_reader: Reader, columns: List[str], dest_table: str) -> None:
        """Dump a csv reader into the given table. """
        sql_columns = ",".join(columns)
        sql_query = f"""COPY {dest_table} ({sql_columns}) FROM STDIN
                        WITH CSV HEADER DELIMITER AS ','
                        NULL AS 'NULL';"""
        raw_conn = self.conn.engine.raw_connection()  # type: ignore
        try:
            raw_conn.cursor().copy_expert(sql_query, csv_reader)
        except Exception:
            raw_conn.rollback()
            raise
        else:
            raw_conn.commit()


# Session context managers:


@contextlib.contextmanager
def bound_session(connection: Connection) -> SessionGenerator:
    Session = sessionmaker()
    sess = Session(bind=connection)
    try:
        yield sess
    finally:
        sess.close()


@contextlib.contextmanager
def atomic_session(connection: Connection) -> SessionGenerator:
    Session = sessionmaker()
    sess = Session(bind=connection)
    try:
        yield sess
    except Exception:
        sess.rollback()
        raise
    else:
        sess.commit()
    finally:
        sess.close()
