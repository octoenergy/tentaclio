from typing import ContextManager

from dataio import credentials, protocols, urls


__all__ = ["open"]


def _open_reader(url: str, mode: str, **kwargs) -> ContextManager[protocols.Reader]:
    """Opens the url and returns a reader """
    authenticated = credentials.load_credentials_injector().inject(urls.URL(url))
    return authenticated.open_reader(mode, extras=kwargs)


def _open_writer(url: str, mode: str, **kwargs) -> ContextManager[protocols.Writer]:
    """Opens the url and returns a writer"""
    authenticated = credentials.load_credentials_injector().inject(urls.URL(url))
    return authenticated.open_writer(mode, extras=kwargs)


def open(url: str, mode: str = None, **kwargs) -> ContextManager[protocols.AnyReaderWriter]:
    """Opens the url and returns a reader or writer depending on mode

    Examples:
        >>> open(path, 'b')  # opens binary reader
        >>> open(path, 'br')  # opens binary reader
        >>> open(path, 'wb')  # opens binary writer
        >>> open(path, 'rw')  # ! raises ValueError due to ambiguity
    """
    mode = mode or ""

    _assert_mode(mode)
    is_write_mode = "w" in mode

    if is_write_mode:
        return _open_writer(url=url, mode=mode, **kwargs)
    else:
        return _open_reader(url=url, mode=mode, **kwargs)


VALID_MODES = ("", "rb", "wb", "rt", "wt", "r", "w", "b", "t")


def _assert_mode(mode: str):
    """Check that mode is valid or raise error otherwise.
    """
    if mode not in VALID_MODES:
        valid_modes = ",".join(VALID_MODES)
        raise ValueError(f"Mode {mode} is not allowed. Valid modes are  {valid_modes}")
