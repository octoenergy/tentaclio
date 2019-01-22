import pytest

from dataio.clients import types


@pytest.fixture()
def fixture_conn():
    class TestConn:
        def close(self):
            pass

    return TestConn()


@pytest.mark.skip("Checked by mypy")
def test_reader(fixture_conn):
    def func(conn: types.Closable):
        pass

    func(fixture_conn)
