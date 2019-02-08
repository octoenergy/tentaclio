from typing import Union

from . import exceptions
from .. import URL
from .sqla_client import SQLAlchemyClient, atomic_session, bound_session


__all__ = ["AthenaClient", "bound_session", "atomic_session"]


class AthenaClient(SQLAlchemyClient):
    """
    Generic Athena hook, backed by a SQLAlchemy connection
    """

    def __init__(
        self, url: Union[str, URL], execution_options: dict = None, connect_args: dict = None
    ) -> None:
        super().__init__(url=url, execution_options=execution_options, connect_args=connect_args)

        if self.url.scheme != "awsathena+rest":
            raise exceptions.SQLAlchemyError(f"Incorrect scheme {self.url.scheme}")
