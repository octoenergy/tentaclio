from . import sqla_client


class DatabricksClient(sqla_client.SQLAlchemyClient):
    """Databricks client, backed by a pyodbc + SQLAlchemy connection"""

    pass
