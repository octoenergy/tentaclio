from typing import Any, ByteString, Generic, Sequence, TypeVar, Union

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
    "CsvDumper",
]

# https://blog.daftcode.pl/covariance-contravariance-and-invariance-the-ultimate-python-guide-8fabc0c24278
# For it's used as a Generic in a protocol T_read needs to be covariant and T_Write contravariant.
T_read = TypeVar("T_read", covariant=True)
T_write = TypeVar("T_write", contravariant=True)


class Closable(Protocol):
    def close(self) -> None:
        ...


class _Reader(Generic[T_read], Protocol):
    def read(self, size: int = -1) -> T_read:
        ...


class _Writer(Generic[T_write], Protocol):
    def write(self, contents: T_write) -> int:
        ...


class Reader(_Reader[Any], Protocol):
    ...


class Writer(_Writer[Any], Protocol):
    ...


class ByteReader(_Reader[ByteString], Protocol):
    ...


class ByteWriter(_Writer[ByteString], Protocol):
    ...


# Attention! Protocol needs to be the last superclass!
# otherwise returning these composed protocols won't work
class ReaderClosable(Reader, Closable, Protocol):
    ...


class WriterClosable(Writer, Closable, Protocol):
    ...


AnyReaderWriter = Union[Reader, Writer]


class CsvDumper(Protocol):
    """Csv into db dumping contract."""

    # The context manager is an actual protocol defined in the
    # std library .... but it can't  be inherited from
    # Also protocols and return types still have gotchas,
    # here PostgresClient won't be recognised as a CsvDumper
    # the reason why we're using any
    # def __enter__(self) -> "CsvDumper":
    def __enter__(self) -> Any:
        ...

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        ...

    def dump_csv(self, csv_reader: Reader, columns: Sequence[str], dest_table: str) -> None:
        ...
