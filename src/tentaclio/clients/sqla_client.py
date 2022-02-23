"""Provide sql connection using sqlalchemy.

This client is used for convinience when using different sql
providers and unifying the client creation. We do not intent to rewriter sqlalchemy.
"""
import contextlib
from typing import Container, Generator, Optional, Union

import pandas as pd
from sqlalchemy.engine import Connection, CursorResult, Engine, create_engine
from sqlalchemy.engine import url as sqla_url
from sqlalchemy.orm import session, sessionmaker
from sqlalchemy.sql.schema import MetaData

from tentaclio import urls

from . import base_client, decorators


__all__ = ["SQLAlchemyClient", "bound_session", "atomic_session"]


SessionGenerator = Generator[None, session.Session, None]


class _TrueContainer(Container[str]):
    """String container that always returns true.

    As we don't have control over the protocols that sqlalchemy is able to
    accept. We shouldn't try to limit which urls can be used here or not.
    So this container will play well with the super class checks to allow
    schemes for different clients.
    """

    def __contains__(self, obj: object) -> bool:
        """Return true."""
        return True


class SQLAlchemyClient(base_client.BaseClient["SQLAlchemyClient"]):
    """SQLAlchemy based client."""

    # The allowed drivers depend on the dependencies installed.
    allowed_schemes: Container[str] = _TrueContainer()
    # Default connect_args
    connect_args_default: dict = {}

    conn: Connection
    engine: Optional[Engine] = None
    execution_options: dict
    connect_args: dict
    database: str
    drivername: str
    username: Optional[str]
    password: Optional[str]
    host: Optional[str]
    port: Optional[int]

    def __init__(
        self, url: Union[str, urls.URL], execution_options: dict = None, connect_args: dict = None
    ) -> None:
        """Create sqlalchemy client based on the passed url.

        This is a wrapper for sqlalchemy engine/connection creation.
        """
        self.execution_options = execution_options or {}
        self.connect_args = connect_args or self.connect_args_default
        super().__init__(url)
        self._extract_url_params()

    def _extract_url_params(self) -> None:
        """Extract the database parameters from the url."""
        # the database doesn't start with /
        database = self.url.path[1:]

        self.database = database
        self.drivername = self.url.scheme
        self.username = self.url.username
        self.password = self.url.password
        self.host = self.url.hostname
        self.port = self.url.port
        self.url_query = self.url.query

    # Connection methods:

    def _connect(self) -> Connection:

        parsed_url = sqla_url.URL(
            drivername=self.drivername,
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            query=self.url_query,
        )
        if self.engine is None:
            self.engine = create_engine(
                parsed_url,
                execution_options=self.execution_options,
                connect_args=self.connect_args,
            )
        return self.engine.connect()

    def _get_raw_conn(self):
        """Acquire raw DBAPI connection from the pool."""
        return self.conn.engine.raw_connection()

    # Schema methods:

    def set_schema(self, meta_data: MetaData) -> None:
        """Create tables based on the metadata object."""
        meta_data.create_all(bind=self.conn)

    def delete_schema(self, meta_data: MetaData) -> None:
        """Delete tables based on the metadata object."""
        meta_data.drop_all(bind=self.conn)

    # Query methods:

    @decorators.check_conn
    def query(self, sql_query: str, **kwargs) -> CursorResult:
        """Execute a read-only SQL query, and return results.

        This will not commit any changes to the database.
        """
        return self.conn.execute(sql_query, **kwargs)

    @decorators.check_conn
    def execute(self, sql_query: str, **kwargs) -> None:
        """Execute a raw SQL query command."""
        trans = self.conn.begin()
        try:
            self.conn.execute(sql_query, **kwargs)
        except Exception:
            trans.rollback()
            raise
        else:
            trans.commit()

    # Dataframe methods:

    @decorators.check_conn
    def get_df(self, sql_query: str, params: dict = None, **kwargs) -> pd.DataFrame:
        """Run a raw SQL query and return a data frame."""
        return pd.read_sql(sql_query, self.conn, params=params, **kwargs)


# Session context managers:


@contextlib.contextmanager
def bound_session(connection: Connection) -> SessionGenerator:
    """Context manager for a sqlalchemy session."""
    Session = sessionmaker()
    sess = Session(bind=connection)
    try:
        yield sess
    finally:
        sess.close()


@contextlib.contextmanager
def atomic_session(connection: Connection) -> SessionGenerator:
    """Context manager for a session that will rollback in case of an exception."""
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
