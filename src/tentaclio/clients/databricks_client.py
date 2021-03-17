"""Databricks query client."""
from typing import Dict
import urllib

from sqlalchemy.engine import Connection, create_engine

from . import sqla_client


class DatabricksClient(sqla_client.SQLAlchemyClient):
    """Databricks client, backed by a pyodbc + SQLAlchemy connection."""

    def _connect(self) -> Connection:
        odbc_connection_map = self._build_odbc_connection_dict()
        connection_url = build_odbc_connection_string(**odbc_connection_map)

        if self.engine is None:
            self.engine = create_engine(
                f"mssql+pyodbc:///?odbc_connect={connection_url}"
            )
        return self.engine.connect()

    def _build_odbc_connection_dict(self) -> Dict:
        odbc_connection_string_map = {
            "UID": "token",
            "PWD": self.username,
            "HOST": self.host,
            "PORT": self.port,
            "Schema": self.database,
        }
        if self.url.query:
            odbc_connection_string_map.update(self.url.query)
        return odbc_connection_string_map


def build_odbc_connection_string(**kwargs) -> str:
    """Build a url formatted odbc connection string from kwargs."""
    connection_url = ";".join([f"{k}={v}" for k, v in kwargs.items()])
    return urllib.parse.quote(connection_url)
