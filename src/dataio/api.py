from typing import ContextManager

from dataio import protocols, urls


__all__ = ["open", "open_reader", "open_writer"]


def open_reader(url: str, mode: str = None, **kwargs) -> ContextManager[protocols.Reader]:
    """Opens the url and returns a reader """
    mode = mode or ""
    return urls.URL(url).open_reader(mode, extras=kwargs)


def open_writer(url: str, mode: str = None, **kwargs) -> ContextManager[protocols.Writer]:
    """Opens the url and returns a writer"""
    mode = mode or ""
    return urls.URL(url).open_writer(mode, extras=kwargs)


def open(url: str, mode: str = None, **kwargs) -> ContextManager:
    """Opens the url and returns a reader or writer depending on mode

    Examples:
        >>> open(path, 'b')  # opens binary reader
        >>> open(path, 'br')  # opens binary reader
        >>> open(path, 'wb')  # opens binary writer
        >>> open(path, 'rw')  # ! raises ValueError due to ambiguity
    """
    mode = mode or ""
    is_read_mode = "r" in mode
    is_write_mode = "w" in mode
    if is_read_mode and is_write_mode:
        raise ValueError(f'Mode must not contain both "r" and "w", found {mode}')
    mode = mode.replace("r", "").replace("w", "")
    if is_write_mode:
        return open_writer(url=url, mode=mode, **kwargs)
    else:
        return open_reader(url=url, mode=mode, **kwargs)
