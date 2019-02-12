import logging

from dataio.protocols import ReaderClosable, WriterClosable
from dataio.urls import URL

logger = logging.getLogger(__name__)


class NullHandler:
    """Handler that raises a NotImplementedError."""
    error_msg_template = "Trying to open {%s} with mode {%s}"

    def _failed_open(self, url: URL, mode: str, extras: dict):
        logger.error(self.error_msg_template, url, mode)
        raise NotImplementedError(self.error_msg_template % (url, mode))

    def open_reader_for(self, url: URL, mode: str, extras: dict) -> ReaderClosable:
        return self._failed_open(url, mode, extras)

    def open_writer_for(self, url: URL, mode: str, extras: dict) -> WriterClosable:
        return self._failed_open(url, mode, extras)
