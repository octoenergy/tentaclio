from .sqla_client import SQLAlchemyClient, bound_session, atomic_session

__all__ = ["AthenaClient", "bound_session", "atomic_session"]


class AthenaClient(SQLAlchemyClient):
    """
    Generic Athena hook, backed by a SQLAlchemy connection
    """

    allowed_schemes = ["awsathena+rest"]
