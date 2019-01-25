from typing import Any, Union

from typing_extensions import Protocol

__all__ = [
    "Closable",
    "Reader",
    "Writer",
    "ReaderWriter",
    "ReaderClosable",
    "WriterClosable",
    "AnyReaderWriter",
]


class Closable(Protocol):
    def close(self) -> None:
        ...


class Reader(Protocol):
    def read(self, size: int = -1) -> Any:
        ...


class Writer(Protocol):
    def write(self, content: Any) -> int:
        ...


class ReaderClosable(Protocol):
    def read(self, size: int = -1) -> Any:
        ...

    def close(self) -> None:
        ...


# Mypy is flaky without this protocol sometimes won't work
class WriterClosable(Protocol):
    def write(self, contents: Any) -> int:
        ...

    def close(self) -> None:
        ...


class ReaderWriter(Protocol):
    def read(self, size: int = -1) -> Any:
        ...

    def write(self, contents: Any) -> int:
        ...


AnyReaderWriter = Union[Reader, Writer, ReaderWriter]
