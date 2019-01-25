from dataio.clients import base_client

import pytest


class TestBaseClient:
    @pytest.mark.skip("")
    def test_client_url_scheme(self):
        url = "file:///path"

        class TestClient(base_client.BaseClient):
            def connect(self):
                return None

        test_client = TestClient(url)

        assert test_client.url.scheme == "file"

    @pytest.mark.skip("")
    def test_closed_client_connection(self, mocker):
        url = "file:///path"
        mocked_conn = mocker.Mock()

        class TestClient(base_client.BaseClient):
            def connect(self):
                return mocked_conn

        with TestClient(url) as test_client:
            pass

        mocked_conn.close.assert_called()
        assert test_client.conn is None
