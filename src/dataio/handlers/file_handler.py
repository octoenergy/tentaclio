import logging

from dataio.protocols import ReaderClosable, WriterClosable
from dataio.urls import URL


logger = logging.getLogger(__name__)


def _get_mode_extras(extras: dict) -> str:
    mode = extras.get("mode", "")
    if "b" in str(mode):
        return "b"
    return ""


class LocalFileHandler:
    """Handler for opening writers and readers for S3 buckets."""

    def open_reader_for(self, url: URL, extras: dict) -> ReaderClosable:
        """Open a local file for reading.
        If the extras argument contains a 'mode' key, it will be used to modify the opening
        mode of the local file. At the moment only 't' or 'b' are checked for the text or
        binary mode.  r/w are implicit from the called function are will be ignored.

        :extras: dictionary with extra arguments for opening the file.
        """
        mode = "r" + _get_mode_extras(extras)
        logger.debug(f"opening {url.path} with mode {mode}")
        return open(url.path, mode)

    def open_writer_for(self, url: URL, extras: dict) -> WriterClosable:
        """Open an s3 bucket for writing.
        If the extras argument contains a 'mode' key, it will be used to modify the opening
        mode of the local file. At the moment only 't' or 'b' are checked for the text or
        binary mode.  r/w are implicit from the called function are will be ignored.

        :extras: dictionary with extra arguments for opening the file.
        """
        mode = "w" + _get_mode_extras(extras)
        logger.debug(f"opening {url.path} with mode {mode}")
        return open(url.path, mode)
