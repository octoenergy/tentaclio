import ftplib
import logging
from typing import Optional, Union, cast

import pysftp

from dataio import protocols
from dataio.urls import URL

from . import decorators, exceptions, stream_client


logger = logging.getLogger(__name__)

__all__ = ["FTPClient", "SFTPClient"]


class FTPClient(stream_client.StreamClient):
    """
    Generic FTP hook
    """

    conn: ftplib.FTP

    def __init__(self, url: Union[str, URL], **kwargs) -> None:
        super().__init__(url)

        if self.url.scheme != "ftp":
            raise exceptions.FTPError(f"Incorrect scheme {self.url.scheme}")

    # Connection methods:

    def connect(self) -> ftplib.FTP:
        logging.info(f"starting ftp connetion to {self.url}")
        return ftplib.FTP(
            self.url.hostname or "", self.url.username or "", self.url.password or ""
        )

    # Stream methods:

    @decorators.check_conn()
    def get(self, writer: protocols.Writer, file_path: str = None) -> None:
        remote_path = file_path or self.url.path
        if remote_path == "":
            raise exceptions.FTPError("Missing remote file path")

        if not self._isfile(remote_path):
            raise exceptions.FTPError("Unable to fetch the remote file")

        self.conn.retrbinary(f"RETR {remote_path}", writer.write)

    @decorators.check_conn()
    def put(self, file_obj: protocols.Reader, file_path: Optional[str] = None) -> None:
        remote_path = file_path or self.url.path
        cast(ftplib.FTP, self.conn).storbinary(f"STOR {remote_path}", file_obj)  # type: ignore

    # Helpers:

    def _isfile(self, file_path: str) -> bool:
        """
        Caveat for missing method on standard FTPlib
        """
        try:
            # Query info
            # https://tools.ietf.org/html/rfc3659#section-7
            cmd = "MLST " + file_path
            self.conn.sendcmd(cmd)
            return True
        except ftplib.error_perm as e:
            logger.error("ftplib error: " + str(e))
            return False


class SFTPClient(stream_client.StreamClient):
    """
    Generic SFTP hook
    """

    conn: pysftp.Connection

    def __init__(self, url: Union[str, URL], **kwargs) -> None:
        super().__init__(url)

        if self.url.scheme != "sftp":
            raise exceptions.FTPError(f"Incorrect scheme {self.url.scheme}")

    # Connection methods:

    def connect(self) -> pysftp.Connection:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        return pysftp.Connection(
            self.url.hostname,
            username=self.url.username,
            password=self.url.password,
            cnopts=cnopts,
            port=self.url.port,
        )

    # Stream methods:

    @decorators.check_conn()
    def get(self, writer: protocols.Writer, file_path: str = None) -> None:
        remote_path = file_path or self.url.path
        if remote_path == "":
            raise exceptions.FTPError("Missing remote file path")

        if not self.conn.isfile(remote_path):
            raise exceptions.FTPError("Unable to fetch the remote file")

        logger.info(f"sftp reading from {remote_path}")
        self.conn.getfo(remote_path, writer)

    @decorators.check_conn()
    def put(self, file_obj: protocols.Reader, file_path: str = None) -> None:
        remote_path = file_path or self.url.path
        if remote_path == "":
            raise exceptions.FTPError("Missing remote file path")

        logger.info(f"sftp writing to {remote_path}")
        # this gives permission
        # self.conn.putfo(remote_path, file_obj.read())
        # but open works
        with self.conn.open(remote_path, mode="wb") as f:
            print("f", f)
            f.write(file_obj.read())
