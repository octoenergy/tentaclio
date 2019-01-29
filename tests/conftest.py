"""
Local py.test plugins

https://docs.pytest.org/en/latest/writing_plugins.html#conftest-py-plugins
"""
import os

import pytest

from dataio import clients


# Database fixtures:


POSTGRES_TEST_URL = os.getenv("POSTGRES_TEST_URL")


@pytest.fixture(scope="session")
def db_client():
    """
    Create and tear down the session-wide SQLAlchemy Db connection
    """
    assert POSTGRES_TEST_URL is not None, "Missing test config in environment variables"
    with clients.PostgresClient(POSTGRES_TEST_URL) as client:
        yield client
