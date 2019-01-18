import contextlib
import tempfile
from typing import Generator, List, Optional, Union

import pandas as pd
from sqlalchemy.engine import Connection, create_engine
from sqlalchemy.engine import url as sqla_url
from sqlalchemy.orm import session, sessionmaker
from sqlalchemy.sql.schema import MetaData

from . import base_client, decorators, exceptions


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

    # Connection methods:

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

    # Schema methods:

    def set_schema(self, meta_data: MetaData) -> None:
        meta_data.create_all(bind=self.conn)

    def delete_schema(self, meta_data: MetaData) -> None:
        meta_data.drop_all(bind=self.conn)

    # Sequal methods:

    @decorators.check_conn()
    def query(self, sql_query: str, **params) -> List[dict]:
        """
        Execute a read-only SQL query, and return results

        Remark: will NOT commit any changes to DB
        """
        raw_result = self.conn.execute(sql_query, params=params)  # type: ignore
        result = [dict(r) for r in raw_result]
        return result

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
        """
        Dump a data frame into an existing Postgres table
        """
        sql_query = f"""
                    COPY {dest_table} ({', '.join(df.columns)}) FROM STDIN
                    WITH CSV HEADER DELIMITER AS ','
                    NULL AS 'NULL';
                    """
        with tempfile.TemporaryFile(mode="w+") as f:
            df.to_csv(f, index=False)
            f.seek(0)
            # Use the raw connection which has the copy_expert method
            raw_conn = self.conn.engine.raw_connection()  # type: ignore
            try:
                raw_conn.cursor().copy_expert(sql_query, f)
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
