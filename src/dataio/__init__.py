from dataio.handlers import (FTPHandler, LocalFileHandler,
                             S3URLHandler, SFTPHandler)

from .protocols import *  # noqa
from .urls import *  # noqa

# Local files
URL.register_handler("", LocalFileHandler())
URL.register_handler("file", LocalFileHandler())

# s3 buckets
URL.register_handler("s3", S3URLHandler())

# ftp buckets
URL.register_handler("ftp", FTPHandler())

# ftp buckets
URL.register_handler("sftp", SFTPHandler())
