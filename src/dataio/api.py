"""Main entry points to octio-io."""
from typing import ContextManager

from dataio import clients, credentials, protocols


__all__ = ["open", "db"]

VALID_MODES = ("", "rb", "wb", "rt", "wt", "r", "w", "b", "t")


def open(url: str, mode: str = None, **kwargs) -> ContextManager[protocols.AnyReaderWriter]:
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


def db(url: str, **kwargs) -> clients.SQLAlchemyClient:
    """Create an authenticated sqlachemy client."""
    authenticated = credentials.authenticate(url)
    return clients.SQLAlchemyClient(authenticated, **kwargs)


# Helpers


def _assert_mode(mode: str):
    """Check if a mode is valid or raise an error otherwise."""
    if mode not in VALID_MODES:
        valid_modes = ",".join(VALID_MODES)
        raise ValueError(f"Mode {mode} is not allowed. Valid modes are  {valid_modes}")


def _open_writer(url: str, mode: str, **kwargs) -> ContextManager[protocols.Writer]:
    """Open a url and return a writer."""
    authenticated = credentials.authenticate(url)
    return authenticated.open_writer(mode, extras=kwargs)


def _open_reader(url: str, mode: str, **kwargs) -> ContextManager[protocols.Reader]:
    """Open a url and return a reader."""
    authenticated = credentials.authenticate(url)
    return authenticated.open_reader(mode, extras=kwargs)
