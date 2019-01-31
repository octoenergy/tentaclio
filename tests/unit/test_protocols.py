# Sanity checks on protocols
# These are not unit test but a check that the typing system works
# for the expected inputs.

import pytest

from dataio import protocols


@pytest.fixture()
def fixture_conn():
    class TestConn:
        def close(self):
            ...

    return TestConn()


class MyReader(object):
    def read(self, i=-1):
        ...


class MyWriter(object):
    def write(self, contents):
        ...


@pytest.mark.skip("Checked by mypy")
def test_closable(fixture_conn):
    def func(conn: protocols.Closable):
        ...

    func(fixture_conn)


@pytest.mark.skip("Checked by mypy")
def test_reader():
    def func(reader: protocols.Reader):
        ...

    func(MyReader())


@pytest.mark.skip("Checked by mypy")
def test_writer():
    def func(writer: protocols.Writer):
        ...

    func(MyWriter())
