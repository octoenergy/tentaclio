from typing import Any, Union

from typing_extensions import Protocol


__all__ = ["Closable", "Reader", "Writer", "ReaderWriter", "AnyReaderWriter"]


class Closable(Protocol):
    def close(self) -> None:
        ...


class Reader(Protocol):
    def read(self, size: int = -1) -> Any:
        ...


class Writer(Protocol):
    def write(self, content: Any) -> int:
        ...


class ReaderWriter(Reader, Writer):
    ...


AnyReaderWriter = Union[Reader, Writer, ReaderWriter]
