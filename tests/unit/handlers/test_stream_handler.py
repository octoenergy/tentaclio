import io

from dataio import URL, Reader
from dataio.clients.base_client import StreamClient
from dataio.handlers.stream_handler import StreamURLHandler


class FakeClient(StreamClient):
    def __init__(self, url: URL, **kwargs):
        self._writer: io.BytesIO = io.BytesIO()

    def connect(self):
        pass

    def __enter__(self):
        pass

    def get(self) -> io.StringIO:
        return io.StringIO("hello")

    def put(self, reader: Reader, **params) -> None:
        self._writer.write(reader.read())
        self._writer.seek(0)


def test_open_reader_for(register_handler):
    handler = StreamURLHandler(FakeClient)

    reader = handler.open_reader_for(URL("registered://my/path"), extras={})

    assert "hello" == reader.read()


def test_open_writer_for(register_handler):
    url = URL("registered://my/path")
    client = FakeClient(url)
    handler = StreamURLHandler(lambda url, **kwargs: client)
    writer = handler.open_writer_for(url, extras={})

    writer.write(bytes("test", "utf-8"))
    writer.close()

    assert client._writer.getvalue().decode("utf-8") == "test"
