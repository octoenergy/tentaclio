from typing import ContextManager

from dataio import protocols, urls

__all__ = ["open_reader", "open_writer"]


def open_reader(url: str, mode: str = None, **kwargs) -> ContextManager[protocols.Reader]:
    """Opens the url and returns a reader """
    mode = mode or ""
    return urls.URL(url).open_reader(mode, extras=kwargs)


def open_writer(url: str, mode: str = None, **kwargs) -> ContextManager[protocols.Writer]:
    """Opens the url and returns a writer"""
    mode = mode or ""
    return urls.URL(url).open_writer(mode, extras=kwargs)
