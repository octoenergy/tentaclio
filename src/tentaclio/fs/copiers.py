"""Define default copier."""
from typing import cast

from tentaclio.protocols import Reader, Writer
from tentaclio.streams.api import open
from tentaclio.urls import URL


__all__ = ["DefaultCopier"]


class DefaultCopier:
    """Copier for those schemas that don't have an specialised implementation."""

    def copy(self, source: URL, dest: URL):
        """Copy the contents of the source url into the dest url."""
        with open(str(source), mode="rb") as reader, open(str(dest), mode="wb") as writer:
            cast(Writer, writer).write(cast(Reader, reader).read())
