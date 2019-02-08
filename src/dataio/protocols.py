from typing import Any

from typing_extensions import Protocol


__all__ = ["Closable", "Reader", "Writer", "ReaderClosable", "WriterClosable"]


class Closable(Protocol):
    def close(self) -> None:
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
