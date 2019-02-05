from typing import Any, Union

from typing_extensions import Protocol

__all__ = [
    "Closable",
    "Reader",
    "Writer",
    "ReaderWriter",
    "ReaderClosable",
    "WriterClosable",
    "BufferReader",
    "BufferWriter",
    "AnyReaderWriter",
]


class Closable(Protocol):
    def close(self) -> None:
        ...


class Seeker(Protocol):
    def seek(self, pos: int = 0, whence: int = 0) -> None:
        ...


class Reader(Protocol):
    def read(self, size: int = -1) -> Any:
        ...


class Writer(Protocol):
    def write(self, content: Any) -> int:
        ...


# Attention! Protocol needs to be the last superclass!
# otherwise returning these composed protocols won't work
class ReaderClosable(Reader, Closable, Protocol):
    ...


class WriterClosable(Writer, Closable, Protocol):
    ...


class BufferWriter(Writer, Closable, Seeker, Protocol):
    ...


class BufferReader(Reader, Closable, Seeker, Protocol):
    ...


class ReaderWriter(Reader, Writer, Protocol):
    ...


AnyReaderWriter = Union[Reader, Writer, ReaderWriter]
