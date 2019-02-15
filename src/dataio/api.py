from typing import ContextManager

from dataio import credentials, protocols, urls


__all__ = ["open"]


def _open_reader(url: str, mode: str = None, **kwargs) -> ContextManager[protocols.Reader]:
    """Opens the url and returns a reader """
    mode = mode or ""
    authenticated = credentials.load_credentials_injector().inject(urls.URL(url))
    return authenticated.open_reader(mode, extras=kwargs)


def _open_writer(url: str, mode: str = None, **kwargs) -> ContextManager[protocols.Writer]:
    """Opens the url and returns a writer"""
    mode = mode or ""
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
    is_read_mode = "r" in mode or "R" in mode
    is_write_mode = "w" in mode or "W" in mode
    if is_read_mode and is_write_mode:
        raise ValueError(f'Mode must not contain both "r" and "w", found {mode}')
    for flag_letter in "rRwW":
        mode = mode.replace(flag_letter, "")
    if is_write_mode:
        return _open_writer(url=url, mode=mode, **kwargs)
    else:
        return _open_reader(url=url, mode=mode, **kwargs)
