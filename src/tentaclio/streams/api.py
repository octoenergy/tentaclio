"""Main entry points to tentaclio-io."""
from typing import ContextManager, Union

from tentaclio import protocols
from tentaclio.credentials import authenticate

from .base_stream import DirtyStreamerWriter, StreamerReader, StreamerWriter
from .stream_registry import STREAM_HANDLER_REGISTRY, _WriterContextManager


__all__ = ["open", "make_empty_safe"]

VALID_MODES = ("", "rb", "wb", "rt", "wt", "r", "w", "b", "t")

AnyContextStreamerReaderWriter = Union[
    ContextManager[StreamerReader], ContextManager[StreamerWriter]
]


def open(url: str, mode: str = None, **kwargs) -> AnyContextStreamerReaderWriter:
    """Open a url and return a reader or writer depending on mode.

    Arguments:
        :mode: similar to built-in open, allowed modes are combinations of "w" for writing
        "r" for reading. "t" for text resources, and "b" for binary. The default is "rt".
    Examples:
        >>> open(path, 'b')  # opens binary reader
        >>> open(path, 'br')  # opens binary reader
        >>> open(path, 'wb')  # opens binary writer
        >>> open(path, 'wt')  # opens text  writer

    """
    mode = mode or ""

    _assert_mode(mode)
    is_write_mode = "w" in mode

    if is_write_mode:
        return _open_writer(url=url, mode=mode, **kwargs)
    else:
        return _open_reader(url=url, mode=mode, **kwargs)


def make_empty_safe(
    context_writer: _WriterContextManager,
) -> ContextManager[protocols.WriterClosable]:
    """Make the writer to not flush the contents if nothing was written."""
    return _WriterContextManager(DirtyStreamerWriter(context_writer.resource))


# Helpers


def _assert_mode(mode: str):
    """Check if a mode is valid or raise an error otherwise."""
    if mode not in VALID_MODES:
        valid_modes = ",".join(VALID_MODES)
        raise ValueError(f"Mode {mode} is not allowed. Valid modes are  {valid_modes}")


def _open_writer(url: str, mode: str, **kwargs) -> ContextManager[StreamerWriter]:
    """Open a url and return a writer."""
    authenticated = authenticate(url)
    return STREAM_HANDLER_REGISTRY.open_stream_writer(authenticated, mode, extras=kwargs)


def _open_reader(url: str, mode: str, **kwargs) -> ContextManager[StreamerReader]:
    """Open a url and return a reader."""
    authenticated = authenticate(url)
    return STREAM_HANDLER_REGISTRY.open_stream_reader(authenticated, mode, extras=kwargs)
