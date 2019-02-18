import io

from dataio import URL, Reader, Writer
from dataio.clients import StreamClient
from dataio.handlers.stream_handler import StreamURLHandler


class FakeClient(StreamClient):
    # clients only understand bytes
    def __init__(self, url: URL, message: bytearray = None):
        self._writer = io.BytesIO()
        self._message = message or bytes("hello", encoding="utf-8")
        self._closed = False

    def _connect(self):
        return None

    def get(self, writer: Writer) -> None:
        writer.write(self._message)

    def put(self, reader: Reader, **params) -> None:
        self._writer.write(reader.read())
        self._writer.seek(0)


def test_open_reader_for_string(register_handler):
    handler = StreamURLHandler(FakeClient)
    reader = handler.open_reader_for(URL("registered://my/path"), mode="t", extras={})
    assert "hello" == reader.read()


def test_open_reader_for_bytes(register_handler):
    message = bytes("hello", "utf-8")
    handler = StreamURLHandler(FakeClient)
    reader = handler.open_reader_for(URL("registered://my/path"), mode="b", extras={})
    assert message == reader.read()


def test_open_writer_for_string(register_handler):
    url = URL("registered://my/path")
    client = FakeClient(url)
    handler = StreamURLHandler(lambda url, **kwargs: client)
    writer = handler.open_writer_for(url, mode="t", extras={})
    writer.write("test")
    writer.close()

    assert client._writer.getvalue().decode("utf-8") == "test"


def test_open_writer_for_bytes(register_handler):
    url = URL("registered://my/path")
    client = FakeClient(url)
    handler = StreamURLHandler(lambda url, **kwargs: client)
    writer = handler.open_writer_for(url, mode="b", extras=dict())

    message = bytes("hello", "utf-8")
    writer.write(message)
    writer.close()

    assert client._writer.getvalue() == message
