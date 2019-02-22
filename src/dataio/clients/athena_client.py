from . import sqla_client


__all__ = ["AthenaClient"]


class AthenaClient(sqla_client.SQLAlchemyClient):
    """
    Generic Athena hook, backed by a SQLAlchemy connection
    """

    allowed_schemes = ["awsathena+rest"]
