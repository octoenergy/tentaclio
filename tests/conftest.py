"""
Local py.test plugins

https://docs.pytest.org/en/latest/writing_plugins.html#conftest-py-plugins
"""
import os

import pytest
from dataio import clients

# Database fixtures:


POSTGRES_TEST_URI = os.getenv("POSTGRES_TEST_URI")


@pytest.yield_fixture(scope="session")
def db_client():
    """
    Create and tear down the session-wide SQLAlchemy Db connection
    """
    assert POSTGRES_TEST_URI is not None
    with clients.PostgresClient(POSTGRES_TEST_URI) as client:
        yield client
