"""
Local py.test plugins

https://docs.pytest.org/en/latest/writing_plugins.html#conftest-py-plugins
"""
import io
import os
from typing import Sequence
from urllib import parse

import pytest

from dataio import URL, Reader, Writer, clients


POSTGRES_TEST_URL = os.getenv("OCTOIO__CONN__POSTGRES_TEST")


# URL fixtures


@pytest.fixture
def sqlite_url():
    db_file = ":memory:"
    url = "sqlite:///" + db_file
    return url


@pytest.fixture(scope="session")
def postgres_url():
    assert POSTGRES_TEST_URL is not None, "Missing postgres URL in environment variables"
    return POSTGRES_TEST_URL


# Client fixtures


@pytest.fixture(scope="session")
def db_client(postgres_url):
    """Create and tear down the session-wide SQLAlchemy Db connection"""
    with clients.PostgresClient(postgres_url) as client:
        yield client


# Handler fixtures


class FakeHandler(object):
    def open_reader_for(self, url: "URL", extras: dict) -> Reader:
        ...

    def open_writer_for(self, url: "URL", extras: dict) -> Writer:
        ...


@pytest.fixture
def fake_handler():
    return FakeHandler()


@pytest.fixture
def register_handler(fake_handler):
    parse.uses_netloc.append("registered")
    URL.register_handler("registered", fake_handler)
    return fake_handler


# Stream fixtures


class CsvDumperRecorder:
    def __enter__(self) -> None:
        pass

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        pass

    def dump_csv(self, csv_reader: Reader, columns: Sequence[str], dest_table: str) -> None:
        self.buff = io.StringIO()
        self.buff.write(csv_reader.read())
        self.buff.seek(0)
        self.columns = columns
        self.dest_table = dest_table


@pytest.fixture
def csv_dumper():
    return CsvDumperRecorder()
