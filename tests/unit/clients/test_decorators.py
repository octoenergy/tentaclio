import pytest

from dataio.clients import base_client, decorators


class TestCheckConn:
    def test_missing_connection_attribute(self):
        class TestClient:
            @decorators.check_conn
            def func(self):
                return True

            def __enter__(self):
                ...

        test_client = TestClient()

        with pytest.raises(AttributeError):
            test_client.func()

    def test_inactive_client_connection(self):
        url = "file:///path"

        class TestClient(base_client.BaseClient):

            allowed_schemes = ["file"]

            def connect(self):
                return True

            @decorators.check_conn
            def func(self):
                return True

            def _connect(self):
                ...

            def __enter__(self):
                ...

        test_client = TestClient(url)

        with pytest.raises(ConnectionError):
            test_client.func()
