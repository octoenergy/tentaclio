"""octo-io is a library that tries to unify how to load, store data.

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
from .protocols import *  # noqa
from .urls import *  # noqa


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

# Assorted SQLAlchemy handlers that don't provide stream readers/writers
URL.register_handler("awsathena+rest", NullHandler())
URL.register_handler("sqlite", NullHandler())

# http / https handlers
URL.register_handler("http", HTTPHandler())
URL.register_handler("https", HTTPHandler())
