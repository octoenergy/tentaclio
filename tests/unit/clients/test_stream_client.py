import io

from dataio import clients


class TestStreamClientWriter:
    def test_write(self, mocker):
        client = mocker.MagicMock()
        buff = io.StringIO()

        writer = clients.StreamClientWriter(client, buff)
        writer.write("hello")
        assert "hello" == buff.getvalue()

    def test_flush_and_close(self, mocker):
        client = mocker.MagicMock()
        buff = io.StringIO()

        writer = clients.StreamClientWriter(client, buff)
        writer.close()

        assert buff.closed
        client.put.assert_called()


class TestStreamClientReader:
    def test_read(self, mocker):
        client = mocker.MagicMock()
        expected = bytes("hello world", "utf-8")
        client.get = lambda f: f.write(expected)

        reader = clients.StreamClientReader(client, io.BytesIO())
        contents = reader.read()
        assert expected == contents

    def test_close(self, mocker):
        client = mocker.MagicMock()

        reader = clients.StreamClientReader(client, io.BytesIO())
        reader.close()

        assert reader.buffer.closed
