import io

import pytest

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


@pytest.mark.parametrize(
    "client, extras, expected",
    [
        (base_stream.StringToBytesClientReader, {}, "utf-8"),
        (base_stream.StringToBytesClientReader, {"encoding": "latin1"}, "latin1"),
        (base_stream.StringToBytesClientWriter, {}, "utf-8"),
        (base_stream.StringToBytesClientWriter, {"encoding": "latin1"}, "latin1"),
    ],
)
def test_setting_encoding_in_string_to_bytes_clients(client, extras, expected, mocker):
    mock_client = mocker.MagicMock()
    mock_inner_buffer = mocker.MagicMock()
    patched_io = mocker.patch("io.TextIOWrapper")
    base_stream.StringToBytesClientReader(mock_client, extras, mock_inner_buffer)
    patched_io.assert_called_with(mock_inner_buffer, encoding=expected)
