"""
Local py.test plugins

https://docs.pytest.org/en/latest/writing_plugins.html#conftest-py-plugins
"""
import os

import pytest

from dataio import URL, Reader, Writer, clients


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


class FakeHandler(object):
    def open_reader_for(self, url: "URL", extras: dict) -> Reader:
        ...

    def open_writer_for(self, url: "URL", extras: dict) -> Writer:
        ...


@pytest.fixture
def register_handler(fake_handler):
    URL.register_handler("registered", fake_handler)
    return fake_handler


@pytest.fixture
def fake_handler():
    return FakeHandler()
