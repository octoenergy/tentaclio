import io

from tentaclio.streams import base_stream


class TestStreamClientWriter:
    def test_write(self, mocker):
        client = mocker.MagicMock()
        buff = io.StringIO()

        writer = base_stream.StreamClientWriter(client, buff)
        writer.write("hello")
        assert "hello" == buff.getvalue()

    def test_flush_and_close(self, mocker):
        client = mocker.MagicMock()
        buff = io.StringIO()

        writer = base_stream.StreamClientWriter(client, buff)
        writer.close()

        assert buff.closed
        client.put.assert_called()


class TestStreamClientReader:
    def test_read(self, mocker):
        client = mocker.MagicMock()
        expected = bytes("hello world", "utf-8")
        client.get = lambda f: f.write(expected)

        reader = base_stream.StreamClientReader(client, io.BytesIO())
        contents = reader.read()
        assert expected == contents

    def test_readline(self, mocker):
        client = mocker.MagicMock()
        expected = bytes("hello world\n", "utf-8")
        retrieved = expected + bytes("this is a new line", "utf-8")
        client.get = lambda f: f.write(retrieved)

        reader = base_stream.StreamClientReader(client, io.BytesIO())
        contents = reader.readline()
        assert expected == contents

    def test_close(self, mocker):
        client = mocker.MagicMock()

        reader = base_stream.StreamClientReader(client, io.BytesIO())
        reader.close()

        assert reader.buffer.closed
