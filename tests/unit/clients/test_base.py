import io

from dataio.clients import StreamClientReader, StreamClientWriter, base_client
from dataio.url import URL


class FakeClient(base_client.BaseClient):
    connetion = None

    def connect(self):
        return self.connection

    def __enter__(self) -> "FakeClient":
        return self


class TestBaseClient:
    def test_create_with_string(self, register_handler):
        url = "registered:///path"
        fake_client = FakeClient(url)

        assert fake_client.url.scheme == "registered"

    def test_creation_with_url(self):
        url = URL("registered:///path")
        fake_client = FakeClient(url)
        assert fake_client.url.scheme == "registered"

    def test_closed_client_connection(self, mocker, register_handler):
        url = "registered:///path"
        mocked_conn = mocker.Mock()
        with FakeClient(url) as fake_client:
            fake_client.conn = mocked_conn
        mocked_conn.close.assert_called()
        assert fake_client.conn is None


class TestStreamClientWriter:
    def test_write(self, mocker):
        client = mocker.MagicMock()
        buff = io.StringIO()

        writer = StreamClientWriter(client, lambda: buff)
        writer.write("hello")
        assert "hello" == buff.getvalue()

    def test_flush_and_close(self, mocker):
        client = mocker.MagicMock()
        buff = io.StringIO()

        writer = StreamClientWriter(client, lambda: buff)
        writer.close()

        assert buff.closed
        client.put.assert_called()


class TestStreamClientReader:
    def test_read(self, mocker):
        client = mocker.MagicMock()
        expected = bytes("hello world", "utf-8")
        client.get.return_value = io.BytesIO(expected)

        reader = StreamClientReader(client)
        contents = reader.read()
        assert expected == contents

    def test_close(self, mocker):
        client = mocker.MagicMock()

        reader = StreamClientReader(client)
        reader.close()

        assert reader.buffer.closed
