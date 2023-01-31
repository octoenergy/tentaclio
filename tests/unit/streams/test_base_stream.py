import io

from tentaclio.streams import base_stream


class TestStreamerWriter:
    def test_write(self, mocker):
        client = mocker.MagicMock()
        buff = io.StringIO()

        writer = base_stream.StreamerWriter(client, buff)
        writer.write("hello")
        assert "hello" == buff.getvalue()

    def test_flush_and_close(self, mocker):
        client = mocker.MagicMock()
        buff = io.StringIO()

        writer = base_stream.StreamerWriter(client, buff)
        writer.close()

        assert buff.closed
        client.put.assert_called()


class TestDirtyStreamerWriter:
    def test_write_dirty(self, mocker):
        client = mocker.MagicMock()
        buff = io.StringIO()

        writer = base_stream.DirtyStreamerWriter(base_stream.StreamerWriter(client, buff))
        writer.write("hello")
        assert "hello" == buff.getvalue()
        writer.close()
        client.put.assert_called()

    def test_write_clean(self, mocker):
        client = mocker.MagicMock()
        buff = io.StringIO()

        writer = base_stream.DirtyStreamerWriter(base_stream.StreamerWriter(client, buff))
        writer.close()

        assert buff.closed
        client.put.assert_not_called()


class TestStreamerReader:
    def test_read(self, mocker):
        client = mocker.MagicMock()
        expected = bytes("hello world", "utf-8")
        client.get = lambda f: f.write(expected)

        reader = base_stream.StreamerReader(client, io.BytesIO())
        contents = reader.read()
        assert expected == contents

    def test_readline(self, mocker):
        client = mocker.MagicMock()
        expected = bytes("hello world\n", "utf-8")
        retrieved = expected + bytes("this is a new line", "utf-8")
        client.get = lambda f: f.write(retrieved)

        reader = base_stream.StreamerReader(client, io.BytesIO())
        contents = reader.readline()
        assert expected == contents

    def test_close(self, mocker):
        client = mocker.MagicMock()

        reader = base_stream.StreamerReader(client, io.BytesIO())
        reader.close()

        assert reader.buffer.closed

    def test_seekable(self, mocker):
        client = mocker.MagicMock()

        reader = base_stream.StreamerReader(client, io.BytesIO())
        assert reader.seekable()
