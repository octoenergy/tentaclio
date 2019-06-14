"""tentaclio is a library that tries to unify how to load, store data.

The main use cases in mind are ETL processes and notebooks, but can be used in many other contexts.
The main benefits are:
    * url based resource management.
    * same function to open readers and writers for resources of different natures.
    Just change the url, the code remains the same.
    * The same for dbs, create clients with ease and use them regardless
    the underlying implementation (thanks to sqlalchemy).
    * Credentials management that allows a distributed credentials storage.
"""

from tentaclio.handlers import *  # noqa

from .api import *  # noqa
from .fs import *  # noqa
from .fs.api import *  # noqa
from .protocols import *  # noqa
from .registries import STREAM_HANDLER_REGISTRY
from .urls import *  # noqa


# Stream handlers

# Local files
STREAM_HANDLER_REGISTRY.register("", LocalFileHandler())
STREAM_HANDLER_REGISTRY.register("file", LocalFileHandler())

# s3 buckets
STREAM_HANDLER_REGISTRY.register("s3", S3URLHandler())

# ftp / sftp handlers
STREAM_HANDLER_REGISTRY.register("ftp", FTPHandler())
STREAM_HANDLER_REGISTRY.register("sftp", SFTPHandler())

# postgres handler
STREAM_HANDLER_REGISTRY.register("postgresql", PostgresURLHandler())

# Assorted SQLAlchemy handlers that doide stream readers/writers
STREAM_HANDLER_REGISTRY.register("awsathena+rest", NullHandler())
STREAM_HANDLER_REGISTRY.register("sqlite", NullHandler())

# http / https handlers
STREAM_HANDLER_REGISTRY.register("http", HTTPHandler())
STREAM_HANDLER_REGISTRY.register("https", HTTPHandler())

# Directory Scanners

SCANNER_REGISTRY.register("", LocalFileScanner())
SCANNER_REGISTRY.register("file", LocalFileScanner())
