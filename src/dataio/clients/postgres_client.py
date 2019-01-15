import contextlib
import typing

import pandas as pd
from sqlalchemy.engine import Connection, create_engine
from sqlalchemy.engine import url as sqla_url
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.schema import MetaData

from . import base_client


__all__ = ["PostgresClient", "bound_session", "atomic_session"]


class PostgresClient(base_client.BaseClient):
    """
    Generic Postgres hook, backed by a SQLAlchemy connection
    """

    conn: typing.Optional[Connection]
    execution_options: dict
    connect_args: dict

    def __init__(
        self,
        url: typing.Union[str, None],
        execution_options: dict = None,
        connect_args: dict = None,
    ) -> None:
        self.execution_options = execution_options or {}
        self.connect_args = connect_args or {}
        super().__init__(url)

    # Connection methods

    def get_conn(self) -> Connection:
        parsed_url = sqla_url.URL(
            drivername=self.url.scheme,
            username=self.url.username,
            password=self.url.password,
            host=self.url.hostname,
            port=self.url.port,
            database=self.url.path,
        )
        engine = create_engine(
            parsed_url, execution_options=self.execution_options, connect_args=self.connect_args
        )
        return engine.connect()

    # Schema methods

    def set_schema(self, meta_data: MetaData) -> None:
        meta_data.create_all(bind=self.conn)

    def delete_schema(self, meta_data: MetaData) -> None:
        meta_data.drop_all(bind=self.conn)

    # Sql methods
    def query(self, sql_query: str, **params) -> typing.List[dict]:
        """
        Execute a read-only SQL query, and return results.

        Will not commit any changes to DB.
        """
        assert self.conn is not None
        raw_result = self.conn.execute(sql_query, params=params)
        result = [dict(r) for r in raw_result]
        return result

    def execute(self, sql_query: str, **params) -> None:
        """
        Execute a raw SQL query command.
        """
        assert self.conn is not None
        trans = self.conn.begin()
        try:
            self.conn.execute(sql_query, params=params)
        except Exception:
            trans.rollback()
            raise

    def get_df(self, sql_query: str, params: dict = None, **kwargs) -> pd.DataFrame:
        """
        Run a raw SQL query and return a dataframe
        """
        assert self.conn is not None
        df = pd.read_sql(sql_query, self.conn, params=params, **kwargs)
        if df.empty:
            raise ValueError("Empty Pandas dataframe content")
        return df


# Session context managers:


@contextlib.contextmanager
def bound_session(connection):
    Session = sessionmaker()
    sess = Session(bind=connection)
    try:
        yield sess
    finally:
        sess.close()


@contextlib.contextmanager
def atomic_session(connection):
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
