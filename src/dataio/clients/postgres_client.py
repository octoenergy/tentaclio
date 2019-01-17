import contextlib
from typing import Generator, List, Optional, Union

import pandas as pd
from sqlalchemy.engine import Connection, create_engine
from sqlalchemy.engine import url as sqla_url
from sqlalchemy.orm import session, sessionmaker
from sqlalchemy.sql.schema import MetaData

from . import base_client

__all__ = ["PostgresClient", "bound_session", "atomic_session"]


NoneString = Union[str, None]
SessionGenerator = Generator[None, session.Session, None]


class PostgresClient(base_client.BaseClient):
    """
    Generic Postgres hook, backed by a SQLAlchemy connection
    """

    conn: Optional[Connection]
    execution_options: dict
    connect_args: dict

    def __init__(
        self, url: NoneString, execution_options: dict = None, connect_args: dict = None
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
    @base_client.check_conn()
    def query(self, sql_query: str, **params) -> List[dict]:
        """
        Execute a read-only SQL query, and return results

        Will not commit any changes to DB.
        """
        raw_result = self.conn.execute(sql_query, params=params)  # type: ignore
        result = [dict(r) for r in raw_result]
        return result

    @base_client.check_conn()
    def execute(self, sql_query: str, **params) -> None:
        """
        Execute a raw SQL query command
        """
        trans = self.conn.begin()  # type: ignore
        try:
            self.conn.execute(sql_query, params=params)  # type: ignore
        except Exception:
            trans.rollback()
            raise

    @base_client.check_conn()
    def get_df(self, sql_query: str, params: dict = None, **kwargs) -> pd.DataFrame:
        """
        Run a raw SQL query and return a data frame
        """
        df = pd.read_sql(sql_query, self.conn, params=params, **kwargs)
        if df.empty:
            raise ValueError("Empty Pandas dataframe content")
        return df


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
