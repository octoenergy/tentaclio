from typing import ContextManager

from dataio import protocols, urls


__all__ = ["open_reader", "open_writer"]


def open_reader(url: str, **kwargs) -> ContextManager[protocols.Reader]:
    """Opens the url and returns a reader """
    return urls.URL(url).open_reader(extras=kwargs)


def open_writer(url: str, **kwargs) -> ContextManager[protocols.Writer]:
    """Opens the url and returns a writer"""
    return urls.URL(url).open_writer(extras=kwargs)
