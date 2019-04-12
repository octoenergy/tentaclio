"""Protocols used in octo-io."""
from typing import Any, ByteString, Generic, TypeVar, Union

from typing_extensions import Protocol


__all__ = [
    "Closable",
    "Reader",
    "Writer",
    "ByteReader",
    "ByteWriter",
    "ReaderClosable",
    "WriterClosable",
    "AnyReaderWriter",
]

# https://blog.daftcode.pl/covariance-contravariance-and-invariance-the-ultimate-python-guide-8fabc0c24278
# For it's used as a Generic in a protocol T_read needs to be covariant and T_Write contravariant.
T_read = TypeVar("T_read", covariant=True)
T_write = TypeVar("T_write", contravariant=True)


class Closable(Protocol):
    """Closable protocol."""

    def close(self) -> None:
        """Close the resource."""
        ...


class _Reader(Generic[T_read], Protocol):
    def read(self, size: int = -1) -> T_read:
        ...


class _Writer(Generic[T_write], Protocol):
    def write(self, contents: T_write) -> int:
        ...


class Reader(_Reader[Any], Protocol):
    """Reader protocol without buffer specialisation."""

    ...


class Writer(_Writer[Any], Protocol):
    """Writer protocol without buffer specialisation."""

    ...


class ByteReader(_Reader[ByteString], Protocol):
    """Reader protocol for bytestrings."""

    ...


class ByteWriter(_Writer[ByteString], Protocol):
    """Writer protocol for bytestrings."""

    ...


# Attention! Protocol needs to be the last superclass!
# otherwise returning these composed protocols won't work
class ReaderClosable(Reader, Closable, Protocol):
    """Composed protocol for reader/closeables."""

    ...


class WriterClosable(Writer, Closable, Protocol):
    """Composed protocol for reader/closeables."""

    ...


AnyReaderWriter = Union[Reader, Writer]
