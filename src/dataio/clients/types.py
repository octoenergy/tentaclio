from typing import TypeVar, Union

from typing_extensions import Protocol


T = TypeVar("T")
NoneString = Union[str, None]


class Closable(Protocol):
    def close(self) -> None:
        ...
