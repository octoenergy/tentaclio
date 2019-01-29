from dataio.clients import base_client
from dataio.url import URL

import pytest


class FakeClient(base_client.BaseClient):
    connetion = None

    def connect(self):
        return self.connection


class TestBaseClient:
    # TODO remove register_handler once file:/// is registered and use parametrize
    # @pytest.mark.parametrize(
    # ["url,scheme"],
    # [
    # ("registered:///path", "registered"),  # from string
    # (URL("registered:///path"), "registered"),  # from url
    # ],
    # )
    # def test_creation_with_url(self, url, scheme):
        # fake_client = FakeClient(url)
        # assert fake_client.url.scheme == scheme

    def test_create_with_string(self, register_handler):
        url = "registered:///path"
        fake_client = FakeClient(url)

        assert fake_client.url.scheme == "registered"

    def test_creation_with_url(self):
        url = URL("registered:///path")
        fake_client = FakeClient(url)
        assert fake_client.url.scheme == "registered"

    @pytest.mark.skip("")
    def test_closed_client_connection(self, mocker):
        url = "file:///path"
        mocked_conn = mocker.Mock()
        fake_client = FakeClient(url)
        fake_client.connection = mocked_conn

        with FakeClient(url) as fake_client:
            pass

        mocked_conn.close.assert_called()
        assert fake_client.conn is None
