import pytest
from tentaclio.clients.databricks_client import (
    build_odbc_connection_string,
    DatabricksClient,
)
from typing import Dict


@pytest.mark.parametrize(
    "url, expected",
    [
        (
            "databricks+pyodbc://my_t0k3n@db_host:443/database",
            {
                "UID": "token",
                "PWD": "my_t0k3n",
                "HOST": "db_host",
                "PORT": 443,
                "Schema": "database",
            }
        ),
        (
            "databricks+pyodbc://my_t0k3n@db_host:443/",
            {
                "UID": "token",
                "PWD": "my_t0k3n",
                "HOST": "db_host",
                "PORT": 443,
                "Schema": "",
            }
        ),
        (
            "databricks+pyodbc://my_t0k3n@db_host:443/database"
            "?HTTPPath=sql/protocolv1/&AuthMech=3&SparkServerType=3"
            "&ThriftTransport=2&SSL=1&IgnoreTransactions=1&DRIVER=/path/to/driver",
            {
                "UID": "token",
                "PWD": "my_t0k3n",
                "HOST": "db_host",
                "PORT": 443,
                "Schema": "database",
                "AuthMech": '3',
                "HTTPPath": "sql/protocolv1/",
                "IgnoreTransactions": "1",
                "SSL": "1",
                "ThriftTransport": "2",
                "SparkServerType": "3",
                "DRIVER": "/path/to/driver"
            }
        )
    ],
)
def test_build_odbc_connection_dict(url: str, expected: Dict):
    output = DatabricksClient(url)._build_odbc_connection_dict()
    assert output == expected


def test_build_odbc_connection_string():
    conn_dict = {"UID": "user", "PWD": "p@ssw0rd", "HOST": "db_host", "PORT": 443}
    output = build_odbc_connection_string(**conn_dict)
    assert output == "UID%3Duser%3BPWD%3Dp%40ssw0rd%3BHOST%3Ddb_host%3BPORT%3D443"
