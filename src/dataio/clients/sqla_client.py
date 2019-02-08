import contextlib
from typing import Generator, Optional, Union

import pandas as pd
from sqlalchemy.engine import Connection, create_engine, result
from sqlalchemy.engine import url as sqla_url
from sqlalchemy.orm import session, sessionmaker
from sqlalchemy.sql.schema import MetaData

from dataio.urls import URL

from . import base_client, decorators, exceptions


__all__ = ["SQLAlchemyClient", "bound_session", "atomic_session"]


SessionGenerator = Generator[None, session.Session, None]


class SQLAlchemyClient(base_client.QueryClient):
    """
    Generic Postgres hook, backed by a SQLAlchemy connection
    """

    conn: Optional[Connection]
    engine = None
    execution_options: dict
    connect_args: dict
    database: str
    drivername: str
    username: Optional[str]
    password: Optional[str]
    host: Optional[str]
    port: Optional[int]

    def __init__(
        self, url: Union[str, URL], execution_options: dict = None, connect_args: dict = None
    ) -> None:
        self.execution_options = execution_options or {}
        self.connect_args = connect_args or {}
        super().__init__(url)

        # the database doesn't start with /
        database = self.url.path[1:]

        self.database = database
        self.drivername = self.url.scheme
        self.username = self.url.username
        self.password = self.url.password
        self.host = self.url.hostname
        self.port = self.url.port

    # Connection methods:

    def connect(self) -> Connection:

        parsed_url = sqla_url.URL(
            drivername=self.drivername,
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )
        if self.engine is None:
            self.engine = create_engine(
                parsed_url,
                execution_options=self.execution_options,
                connect_args=self.connect_args,
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
            raise exceptions.SQLAlchemyError("Empty Pandas dataframe content")
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
