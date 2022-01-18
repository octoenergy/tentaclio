"""Stream handler registry to open readers and writers to urls."""
from typing import ClassVar, ContextManager, Optional, Protocol

from tentaclio import protocols
from tentaclio.registry import URLHandlerRegistry
from tentaclio.urls import URL


__all__ = ["STREAM_HANDLER_REGISTRY"]


class _ReaderContextManager(ContextManager[protocols.Reader]):
    """Composed context manager for ReaderCloseables."""

    def __init__(self, resource: protocols.ReaderClosable):
        super(_ReaderContextManager, self).__init__()
        self.resource = resource

    def __enter__(self) -> protocols.Reader:
        return self.resource

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return self.resource.close()


class _WriterContextManager(ContextManager[protocols.Writer]):
    """Composed context manager for WriterCloseables."""

    def __init__(self, resource: protocols.WriterClosable):
        super(_WriterContextManager, self).__init__()
        self.resource = resource

    def __enter__(self) -> protocols.Writer:
        return self.resource

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return self.resource.close()


class StreamHandler(Protocol):
    """Protocol for handling stream resources."""

    def open_reader_for(self, url: "URL", mode: str, extras: dict) -> protocols.ReaderClosable:
        """Open a reader for the given url."""
        ...

    def open_writer_for(self, url: "URL", mode: str, extras: dict) -> protocols.WriterClosable:
        """Open a writer for the given url."""
        ...


class StreamHandlerRegistry(URLHandlerRegistry[StreamHandler]):
    """Registry for stream handlers."""

    def open_stream_reader(
        self, url: URL, mode: str, extras: Optional[dict] = None
    ) -> ContextManager[protocols.Reader]:
        """Open a reader for the stream located at this url."""
        extras = extras or {}
        reader = self.get_handler(url.scheme).open_reader_for(url, mode, extras)
        return _ReaderContextManager(reader)

    def open_stream_writer(
        self, url: URL, mode: str, extras: Optional[dict] = None
    ) -> ContextManager[protocols.Writer]:
        """Open a writer for the stream located at this url."""
        extras = extras or {}
        writer = self.get_handler(url.scheme).open_writer_for(url, mode, extras)
        return _WriterContextManager(writer)


class _StreamRegistryHolder:
    """Module level singleton."""

    instance: ClassVar[StreamHandlerRegistry] = StreamHandlerRegistry()


STREAM_HANDLER_REGISTRY = _StreamRegistryHolder().instance
