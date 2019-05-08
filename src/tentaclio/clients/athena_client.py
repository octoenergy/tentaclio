"""AWS Athena query client.

Overrides the `get_df` convenience methods for loading a DataFrame using PandasCursor,
which is more performant than using sql alchemy functions.
"""

import pandas as pd
from pyathena.pandas_cursor import PandasCursor


from . import decorators, sqla_client


__all__ = ["AthenaClient"]


class AthenaClient(sqla_client.SQLAlchemyClient):
    """Postgres client, backed by a SQLAlchemy connection."""

    allowed_schemes = ["awsathena+rest"]

    # Athena-specific fast query result retrieval:

    @decorators.check_conn
    def get_df(self, sql_query: str, params: dict = None, **kwargs) -> pd.DataFrame:
        """Run a raw SQL query and return a data frame."""
        cursor = self.conn.connection.cursor(PandasCursor)
        return cursor.execute(sql_query, parameters=params, **kwargs).as_pandas()
