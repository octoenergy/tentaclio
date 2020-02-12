"""FTP ans SFTP stream clients."""
import ftplib
import io
import logging
import stat
from typing import Iterable, Optional, Union

import pysftp

from tentaclio import fs, protocols, urls

from . import base_client, decorators, exceptions


logger = logging.getLogger(__name__)

__all__ = ["FTPClient", "SFTPClient"]


class FTPClient(base_client.BaseClient["FTPClient"]):
    """Generic FTP client."""

    allowed_schemes = ["ftp"]

    conn: ftplib.FTP

    def __init__(self, url: Union[str, urls.URL], **kwargs) -> None:
        """Create a new ftp client based on the url starting with ftp://."""
        super().__init__(url)
        self.port = self.url.port or 21

    # Connection methods:

    def _connect(self) -> ftplib.FTP:
        logging.info(f"starting ftp connection to {self.url}")
        conn = ftplib.FTP()
        conn.connect(self.url.hostname or "", port=self.port)
        conn.login(user=self.url.username or "", passwd=self.url.password or "")
        return conn

    # Stream methods:

    @decorators.check_conn
    def get(self, writer: protocols.ByteWriter, file_path: str = None) -> None:
        """Write the contents of a remote file into the passed writer.

        Arguments:
            :file_path: The path of the remote file if not passed
                in the costructor as part of the url.
        """
        remote_path = file_path or self.url.path
        if remote_path == "":
            raise exceptions.FTPError("Missing remote file path")

        if not self._isfile(remote_path):
            raise exceptions.FTPError("Unable to fetch the remote file")

        self.conn.retrbinary(f"RETR {remote_path}", writer.write)

    @decorators.check_conn
    def put(self, reader: protocols.ByteReader, file_path: Optional[str] = None) -> None:
        """Write the contents of the reader into the remote file.

        Arguments:
            :file_path: The path of the remote file if not passed
                in the costructor as part of the url.
        """
        remote_path = file_path or self.url.path
        # storebinary only works with io.BytesIO
        buff = io.BytesIO(bytearray(reader.read()))
        self.conn.storbinary(f"STOR {remote_path}", buff)

    # Helpers:

    def _isfile(self, file_path: str) -> bool:
        """Check if the path exists in the remote server."""
        # TODO Shall we just wait for the server to complain?
        try:
            # Query info
            # https://tools.ietf.org/html/rfc3659#section-7
            cmd = "MLST " + file_path
            self.conn.sendcmd(cmd)
            return True
        except ftplib.error_perm as e:
            logger.error("ftplib error: " + str(e))
            return False

    def scandir(self, **kwargs) -> Iterable[fs.DirEntry]:
        """Scan the connection url to create dir entries."""
        base_url = f"ftp://{self.url.hostname}:{self.port}{self.url.path}/"
        entries = []
        for mlsd_entry in self.conn.mlsd(self.url.path, facts=["type"]):
            # https://docs.python.org/3/library/ftplib.html#ftplib.FTP.mlsd
            file_name = mlsd_entry[0]
            entry_type = mlsd_entry[1]["type"]

            url = urls.URL(base_url + file_name)
            if entry_type == "dir":
                entries.append(fs.build_folder_entry(url))
            else:
                entries.append(fs.build_file_entry(url))

        return entries

    @decorators.check_conn
    def remove(self):
        """Remove the file from the ftp."""
        self.conn.delete(self.url.path)


class SFTPClient(base_client.BaseClient["SFTPClient"]):
    """SFTP stream client."""

    allowed_schemes = ["sftp"]

    conn: pysftp.Connection
    username: str
    password: str
    port: int

    def __init__(self, url: Union[str, urls.URL], **kwargs) -> None:
        """Create a new sftp client based on the url starting with sftp://."""
        super().__init__(url)
        self.username = self.url.username or ""
        self.password = self.url.password or ""
        self.port = self.url.port or 22

    # Connection methods:

    def _connect(self) -> pysftp.Connection:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        return pysftp.Connection(
            self.url.hostname,
            username=self.username,
            password=self.password,
            cnopts=cnopts,
            port=self.port,
        )

    # Stream methods:

    @decorators.check_conn
    def get(self, writer: protocols.ByteWriter, file_path: str = None) -> None:
        """Write the contents of a remote file into the passed writer.

        Arguments:
            :file_path: The path of the remote file if not passed
                in the costructor as part of the url.
        """
        remote_path = file_path or self.url.path
        if remote_path == "":
            raise exceptions.FTPError("Missing remote file path")

        if not self.conn.isfile(remote_path):
            raise exceptions.FTPError("Unable to fetch the remote file")

        logger.info(f"sftp reading from {remote_path}")
        self.conn.getfo(remote_path, writer)

    @decorators.check_conn
    def put(self, reader: protocols.ByteReader, file_path: str = None) -> None:
        """Write the contents of the reader into the remote file.

        Arguments:
            :file_path: The path of the remote file if not passed
                in the costructor as part of the url.
        """
        remote_path = file_path or self.url.path
        if remote_path == "":
            raise exceptions.FTPError("Missing remote file path")

        logger.info(f"sftp writing to {remote_path}")
        # this gives permission error
        # self.conn.putfo(remote_path, file_obj.read())
        # but open works
        with self.conn.open(remote_path, mode="wb") as f:
            f.write(reader.read())

    def scandir(self, **kwargs) -> Iterable[fs.DirEntry]:
        """Scan the connection url to create dir entries."""
        base_url = f"sftp://{self.url.hostname}:{self.port}{self.url.path}/"
        entries = []
        for attrs in self.conn.listdir_attr(self.url.path):
            url = urls.URL(base_url + attrs.filename)
            if stat.S_ISDIR(attrs.st_mode):
                entries.append(fs.build_folder_entry(url))

            elif stat.S_ISREG(attrs.st_mode):
                entries.append(fs.build_file_entry(url))
            else:
                continue  # ignore other type of entries
        return entries

    @decorators.check_conn
    def remove(self):
        """Remove the file from the ftp."""
        self.conn.remove(self.url.path)
