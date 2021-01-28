import io

import pytest

from tentaclio.clients import base_client
from tentaclio.urls import URL


class FakeClient(base_client.BaseClient):
    connetion = None

    allowed_schemes = "scheme"

    def _connect(self):
        # return a closable
        return io.StringIO()

    def __enter__(self) -> "FakeClient":
        self.conn = self.connect()
        return self


class TestBaseClient:
    def test_create_with_string(self):
        url = "scheme:///path"
        fake_client = FakeClient(url)

        assert fake_client.url.scheme == "scheme"

    def test_creation_with_url(self):
        url = URL("scheme:///path")
        fake_client = FakeClient(url)
        assert fake_client.url.scheme == "scheme"

    def test_closed_client_connection(self, mocker):
        url = "scheme:///path"
        mocked_conn = mocker.Mock()
        with FakeClient(url) as fake_client:
            fake_client.conn = mocked_conn
        mocked_conn.close.assert_called()
        assert fake_client.closed

    def test_closed_if_connection_not_opened(self, mocker):
        url = "scheme:///path"
        fake_client = FakeClient(url)
        assert fake_client.closed

    def test_client_connected_is_not_closed(self, mocker):
        url = "scheme:///path"
        mocked_conn = mocker.Mock()
        with FakeClient(url) as fake_client:
            fake_client.conn = mocked_conn
            assert not fake_client.closed


class TestLazyExceptionRaiser:
    def test_call(self):
        with pytest.raises(Exception, match="Test exception"):
            base_client._LazyExceptionRaiser(Exception("Test exception"))()

    def test_getattr_field(self):
        # This shouldn't blow as we might want to access class definitions in the client code
        base_client._LazyExceptionRaiser(Exception("Test exception")).this_is_a_field

    def test_getattr_method(self):
        with pytest.raises(Exception, match="Test exception"):
            base_client._LazyExceptionRaiser(Exception("Test exception")).this_is_a_method()


def test_lazy_import_error():
    lazy_error = base_client.lazy_import_error("module_name", Exception("Test exception"))

    try:
        lazy_error()
    except Exception as e:
        assert "module_name" in str(e)
        assert "Test exception" in str(e)
