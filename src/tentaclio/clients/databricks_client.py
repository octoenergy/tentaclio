import urllib

from sqlalchemy.engine import Connection, create_engine

from . import sqla_client


class DatabricksClient(sqla_client.SQLAlchemyClient):
    """Databricks client, backed by a pyodbc + SQLAlchemy connection"""

    DRIVER = "/Library/simba/spark/lib/libsparkodbc_sbu.dylib"

    def _connect(self) -> Connection:
        connection_url = _build_odbc_connection_string(
            DRIVER=self.DRIVER,
            HOST=self.host,
            PORT=self.port,
            UID=self.username,
            PWD=self.password,
            Schema=self.database,
            HTTPPath=self.url.query["http_path"],
            AuthMech=3,
            SparkServerType=3,
            ThriftTransport=2,
            SSL=1,
            IgnoreTransactions=1,
        )

        if self.engine is None:
            self.engine = create_engine(
                f"mssql+pyodbc:///?odbc_connect={connection_url}"
            )
        return self.engine.connect()


def build_odbc_connection_string(**kwargs) -> str:
    connection_url = ";".join([f"{k}={v}" for k, v in kwargs.items()])
    return urllib.parse.quote(connection_url)
