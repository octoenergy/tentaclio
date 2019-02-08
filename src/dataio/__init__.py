from dataio.handlers import *  # noqa

from .protocols import *  # noqa
from .urls import *  # noqa
from .api import *  # noqa

# Local files
URL.register_handler("", LocalFileHandler())
URL.register_handler("file", LocalFileHandler())

# s3 buckets
URL.register_handler("s3", S3URLHandler())

# ftp / sftp handlers
URL.register_handler("ftp", FTPHandler())
URL.register_handler("sftp", SFTPHandler())

# postgres handler
URL.register_handler("postgresql", PostgresURLHandler())

# AWS Athena handler
URL.register_handler("awsathena+rest", NullHandler())

# http / https handlers
URL.register_handler("http", HTTPHandler())
URL.register_handler("https", HTTPHandler())
