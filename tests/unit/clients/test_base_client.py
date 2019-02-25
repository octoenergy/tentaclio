import io

from dataio.clients import base_client
from dataio.urls import URL


class FakeClient(base_client.BaseClient):
    connetion = None

    allowed_schemes = "registered"

    def _connect(self):
        # return a closable
        return io.StringIO()

    def __enter__(self) -> "FakeClient":
        self.conn = self.connect()
        return self


class TestBaseClient:
    def test_create_with_string(self, register_handler):
        url = "registered:///path"
        fake_client = FakeClient(url)

        assert fake_client.url.scheme == "registered"

    def test_creation_with_url(self, register_handler):
        url = URL("registered:///path")
        fake_client = FakeClient(url)
        assert fake_client.url.scheme == "registered"

    def test_closed_client_connection(self, mocker, register_handler):
        url = "registered:///path"
        mocked_conn = mocker.Mock()
        with FakeClient(url) as fake_client:
            fake_client.conn = mocked_conn
        mocked_conn.close.assert_called()
        assert fake_client.closed

    def test_closed_if_connection_not_opened(self, mocker, register_handler):
        url = "registered:///path"
        fake_client = FakeClient(url)
        assert fake_client.closed

    def test_client_connected_is_not_closed(self, mocker, register_handler):
        url = "registered:///path"
        mocked_conn = mocker.Mock()
        with FakeClient(url) as fake_client:
            fake_client.conn = mocked_conn
            assert not fake_client.closed
