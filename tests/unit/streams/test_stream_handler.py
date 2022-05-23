import io

import pytest

from tentaclio import URL, Reader, Writer
from tentaclio.clients import base_client
from tentaclio.streams import StreamURLHandler


class FakeClient(base_client.BaseClient["FakeClient"]):
    # clients only understand bytes
    def __init__(self, url: URL, message: bytearray = None, *args, **kwargs):
        self._writer = io.BytesIO()
        self._message = message or bytes("hello", encoding="utf-8")
        self._closed = False

    def _connect(self):
        # return a closable
        return io.BytesIO()

    def get(self, writer: Writer) -> None:
        writer.write(self._message)

    def put(self, reader: Reader, **params) -> None:
        self._writer.write(reader.read())
        self._writer.seek(0)


def test_open_reader_for_string():
    handler = StreamURLHandler(FakeClient)
    reader = handler.open_reader_for(URL("scheme://my/path"), mode="t", extras={})
    assert "hello" == reader.read()


def test_open_reader_for_bytes():
    message = bytes("hello", "utf-8")
    handler = StreamURLHandler(FakeClient)
    reader = handler.open_reader_for(URL("scheme://my/path"), mode="b", extras={})
    assert message == reader.read()


def test_open_writer_for_string():
    url = URL("scheme://my/path")
    client = FakeClient(url)
    handler = StreamURLHandler(lambda url, **kwargs: client)
    writer = handler.open_writer_for(url, mode="t", extras={})
    writer.write("test")
    writer.close()

    assert client._writer.getvalue().decode("utf-8") == "test"


def test_open_writer_for_bytes():
    url = URL("scheme://my/path")
    client = FakeClient(url)
    handler = StreamURLHandler(lambda url, **kwargs: client)
    writer = handler.open_writer_for(url, mode="b", extras=dict())

    message = bytes("hello", "utf-8")
    writer.write(message)
    writer.close()

    assert client._writer.getvalue() == message


@pytest.mark.parametrize("extras, expected", [({}, "utf-8"), ({"encoding": "latin1"}, "latin1")])
def test_open_reader_with_encoding(mocker, extras, expected):
    handler = StreamURLHandler(FakeClient)
    handler.client_factory = mocker.MagicMock()
    mock_client = mocker.MagicMock()
    handler.client_factory.return_value = mock_client
    mocked_reader = mocker.patch("tentaclio.streams.base_stream.StringToBytesClientReader")
    handler.open_reader_for(URL("scheme://my/path"), mode="t", extras=extras)
    mocked_reader.assert_called_with(mock_client, encoding=expected)


@pytest.mark.parametrize("extras, expected", [({}, "utf-8"), ({"encoding": "latin1"}, "latin1")])
def test_open_writer_with_encoding(mocker, extras, expected):
    handler = StreamURLHandler(FakeClient)
    handler.client_factory = mocker.MagicMock()
    mock_client = mocker.MagicMock()
    handler.client_factory.return_value = mock_client
    mock_writer = mocker.patch("tentaclio.streams.base_stream.StringToBytesClientWriter")
    handler.open_writer_for(URL("scheme://my/path"), mode="t", extras=extras)
    mock_writer.assert_called_with(mock_client, encoding=expected)
