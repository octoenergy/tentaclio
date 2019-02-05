from dataio.clients import ftp_client

from .stream_handler import StreamURLHandler


class FTPHandler(StreamURLHandler):
    """Handler for opening writers and readers for files in ftp servers"""

    def __init__(self):
        super().__init__(ftp_client.FTPClient)


class SFTPHandler(StreamURLHandler):
    """Handler for opening writers and readers for sftp files"""

    def __init__(self):
        super().__init__(ftp_client.SFTPClient)
