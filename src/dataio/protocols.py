from typing import TypeVar, Union

from typing_extensions import Protocol


__all__ = ["Reader", "Writer", "ReaderWriter", "AnyReaderWriter"]


T = TypeVar("T")


class Reader(Protocol):
    def read(self, size: int = -1) -> T:
        ...


class Writer(Protocol):
    def write(self, content: T) -> int:
        ...


class ReaderWriter(Reader, Writer):
    ...


AnyReaderWriter = Union[Reader, Writer, ReaderWriter]
