"""Stream handler registry to open readers and writers to urls."""
from typing import ClassVar, ContextManager, Optional, Protocol

from tentaclio.registry import URLHandlerRegistry
from tentaclio.streams import base_stream
from tentaclio.urls import URL


__all__ = ["STREAM_HANDLER_REGISTRY"]


class _ReaderContextManager(ContextManager[base_stream.StreamerReader]):
    """Composed context manager for ReaderCloseables."""

    def __init__(self, resource: base_stream.StreamerReader):
        super(_ReaderContextManager, self).__init__()
        self.resource = resource

    def __enter__(self) -> base_stream.StreamerReader:
        return self.resource

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.resource.close()


class _WriterContextManager(ContextManager[base_stream.StreamerWriter]):
    """Composed context manager for WriterCloseables."""

    def __init__(self, resource: base_stream.StreamerWriter):
        super(_WriterContextManager, self).__init__()
        self.resource = resource

    def __enter__(self) -> base_stream.StreamerWriter:
        return self.resource

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.resource.close()


class StreamHandler(Protocol):
    """Protocol for handling stream resources."""

    def open_reader_for(self, url: "URL", mode: str, extras: dict) -> base_stream.StreamerReader:
        """Open a reader for the given url."""
        ...

    def open_writer_for(self, url: "URL", mode: str, extras: dict) -> base_stream.StreamerWriter:
        """Open a writer for the given url."""
        ...


class StreamHandlerRegistry(URLHandlerRegistry[StreamHandler]):
    """Registry for stream handlers."""

    def open_stream_reader(
        self, url: URL, mode: str, extras: Optional[dict] = None
    ) -> ContextManager[base_stream.StreamerReader]:
        """Open a reader for the stream located at this url."""
        extras = extras or {}
        reader = self.get_handler(url.scheme).open_reader_for(url, mode, extras)
        return _ReaderContextManager(reader)

    def open_stream_writer(
        self, url: URL, mode: str, extras: Optional[dict] = None
    ) -> ContextManager[base_stream.StreamerWriter]:
        """Open a writer for the stream located at this url."""
        extras = extras or {}
        writer = self.get_handler(url.scheme).open_writer_for(url, mode, extras)
        return _WriterContextManager(writer)


class _StreamRegistryHolder:
    """Module level singleton."""

    instance: ClassVar[StreamHandlerRegistry] = StreamHandlerRegistry()


STREAM_HANDLER_REGISTRY = _StreamRegistryHolder().instance
