import pytest
from dataio.clients import base_client, decorators, exceptions


class TestCheckConn:
    def test_missing_connection_attribute(self):
        class TestClient:
            @decorators.check_conn()
            def func(self):
                return True

            def __enter__(self):
                pass

        test_client = TestClient()

        with pytest.raises(AttributeError):
            test_client.func()

    def test_inactive_client_connection(self):
        url = "file:///path"

        class TestClient(base_client.BaseClient):
            def connect(self):
                return True

            @decorators.check_conn()
            def func(self):
                return True

            def __enter__(self):
                pass

        test_client = TestClient(url)

        with pytest.raises(exceptions.ConnectionError):
            test_client.func()
