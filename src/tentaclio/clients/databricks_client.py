import logging
import os
import platform
import urllib
from typing import Union, Dict, Optional

from sqlalchemy.engine import Connection, create_engine, result

from tentaclio import urls

from . import sqla_client


DRIVER_URL_QUERY_KEY = "simba_driver"
DRIVER_ENV_VAR_KEY = "TENTACLIO__SIMBA_DRIVER"

HTTP_PATH_QUERY_KEY = "http_path"
HTTP_ENV_VAR_KEY = "TENTACLIO__DATABRICKS_HTTP_PATH"


DRIVER_DEFAULT_PATHS = {
    "Windows": "path/to/simba/",
    "Darwin": "/Library/simba/spark/lib/libsparkodbc_sbu.dylib",
    "Linux": "/opt/simba/spark/lib/64/libsparkodbc_sb64.so",
}


logger = logging.getLogger(__name__)


class DatabricksClient(sqla_client.SQLAlchemyClient):
    """Databricks client, backed by a pyodbc + SQLAlchemy connection"""

    def __init__(self, url: Union[str, urls.URL]) -> None:
        super().__init__(url)
        self.driver = self._get_driver_path()
        self.http_path = self._get_http_path()

    def _get_driver_path(
        self,
        env_var_key: str = DRIVER_ENV_VAR_KEY,
        url_query_key: str = DRIVER_URL_QUERY_KEY,
    ) -> str:
        driver_path = get_param_from_url_query_or_env_var(
            env_var_key, url_query_key, self.url.query
        )
        if driver_path == "":
            platform_system = platform.system()
            driver_path = DRIVER_DEFAULT_PATHS[platform_system]
            logger.warning(
                f"Using default driver path for {platform_system}: {driver_path}"
            )
        return driver_path

    def _get_http_path(
        self,
        env_var_key: str = HTTP_ENV_VAR_KEY,
        url_query_key: str = HTTP_PATH_QUERY_KEY,
    ):
        http_path = get_param_from_url_query_or_env_var(
            env_var_key, url_query_key, self.url.query
        )
        if http_path == "":
            raise KeyError("http_path required to connect to Databricks")
        return http_path

    def _connect(self) -> Connection:
        connection_url = build_odbc_connection_string(
            DRIVER=self.driver,
            HOST=self.host,
            PORT=self.port,
            UID=self.username,
            PWD=self.password,
            Schema=self.database,
            HTTPPath=self.http_path,
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

    def execute(self, sql_query: str, params: dict = None, **kwargs) -> None:
        if params is not None:
            logger.warning("Executing without params due to pyodbc limitation")
        return super().execute(sql_query, pass_params=False, **kwargs)

    def query(
        self, sql_query: str, params: dict = None, pass_params: bool = True, **kwargs
    ) -> result.ResultProxy:
        if params is not None:
            logger.warning("Querying without params due to pyodbc limiation")
        return super().query(sql_query, pass_params=False, **kwargs)


def build_odbc_connection_string(**kwargs) -> str:
    connection_url = ";".join([f"{k}={v}" for k, v in kwargs.items()])
    return urllib.parse.quote(connection_url)


def get_param_from_url_query_or_env_var(
    env_var_key: str, url_query_key: str, url_query: Optional[Dict]
) -> str:
    param = os.environ.get(env_var_key, "")
    if param == "" and url_query:
        param = url_query.get(url_query_key, "")
    if param == "":
        logger.warning(
            f"{env_var_key} not found in env vars and {url_query_key} not found in url query"
        )
    return param
