import abc

from dataio import protocols


class BaseBuffer:
    """
    Abstract base class for wrapping a file buffer
    """

    # Context manager:

    @abc.abstractmethod
    def __enter__(self) -> protocols.AnyReaderWriter:
        ...

    @abc.abstractmethod
    def __exit__(self, *args) -> None:
        ...
