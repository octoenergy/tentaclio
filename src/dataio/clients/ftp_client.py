import ftplib
import io
from typing import Optional, Union

import pysftp
from dataio.protocols import Reader
from dataio.urls import URL

from . import base_client, decorators, exceptions

__all__ = ["FTPClient", "SFTPClient"]


class FTPClient(base_client.StreamClient):
    """
    Generic FTP hook
    """

    conn: Optional[ftplib.FTP]

    def __init__(self, url: Union[str, URL]) -> None:
        super().__init__(url)

        if self.url.scheme != "ftp":
            raise exceptions.FTPError(f"Incorrect scheme {self.url.scheme}")

    # Connection methods:

    def connect(self) -> ftplib.FTP:
        return ftplib.FTP(
            self.url.hostname or "", self.url.username or "", self.url.password or ""
        )

    # Stream methods:

    @decorators.check_conn()
    def get(self, file_path: str = None) -> io.BytesIO:
        remote_path = file_path or self.url.path
        if remote_path == "":
            raise exceptions.FTPError("Missing remote file path")

        if not self._isfile(remote_path):
            raise exceptions.FTPError("Unable to fetch the remote file")

        f = io.BytesIO()
        self.conn.retrbinary("RETR %s" % remote_path, f.write)  # type: ignore
        f.seek(0)
        return f

    def put(self, file_obj: Reader, **params) -> None:
        raise NotImplementedError

    # Helpers:

    def _isfile(self, file_path: str) -> bool:
        """
        Caveat for missing method on standard FTPlib
        """
        try:
            # Query info
            # https://tools.ietf.org/html/rfc3659#section-7
            cmd = "MLST " + file_path
            self.conn.sendcmd(cmd)  # type: ignore
            return True
        except ftplib.error_perm:
            return False


class SFTPClient(base_client.StreamClient):
    """
    Generic SFTP hook
    """

    conn: Optional[pysftp.Connection]

    def __init__(self, url: Union[str, URL]) -> None:
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
    def get(self, file_path: str = None) -> io.BytesIO:
        remote_path = file_path or self.url.path
        if remote_path == "":
            raise exceptions.FTPError("Missing remote file path")

        if not self.conn.isfile(remote_path):  # type: ignore
            raise exceptions.FTPError("Unable to fetch the remote file")

        f = io.BytesIO()
        self.conn.getfo(remote_path, f, callback=None)  # type: ignore
        f.seek(0)
        return f

    def put(self, file_obj: Reader, **params) -> None:
        raise NotImplementedError
