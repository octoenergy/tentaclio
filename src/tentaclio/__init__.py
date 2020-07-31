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

from .clients import *  # noqa
from .databases import *  # noqa
from .databases.api import *  # noqa
from .fs import *  # noqa
from .fs.api import *  # noqa
from .protocols import *  # noqa
from .streams import *  # noqa
from .urls import *  # noqa


# Stream handlers

# Local files
STREAM_HANDLER_REGISTRY.register("", StreamURLHandler(LocalFSClient))
STREAM_HANDLER_REGISTRY.register("file", StreamURLHandler(LocalFSClient))

# s3 buckets
STREAM_HANDLER_REGISTRY.register("s3", StreamURLHandler(S3Client))

# gs handlers
STREAM_HANDLER_REGISTRY.register("gs", StreamURLHandler(GSClient))
STREAM_HANDLER_REGISTRY.register("gcs", StreamURLHandler(GSClient))

# ftp / sftp handlers
STREAM_HANDLER_REGISTRY.register("ftp", StreamURLHandler(FTPClient))
STREAM_HANDLER_REGISTRY.register("sftp", StreamURLHandler(SFTPClient))

# postgres handler
STREAM_HANDLER_REGISTRY.register("postgresql", PostgresURLHandler())

# http / https handlers
STREAM_HANDLER_REGISTRY.register("http", StreamURLHandler(HTTPClient))
STREAM_HANDLER_REGISTRY.register("https", StreamURLHandler(HTTPClient))

# google drive handlers
STREAM_HANDLER_REGISTRY.register("googledrive", StreamURLHandler(GoogleDriveFSClient))
STREAM_HANDLER_REGISTRY.register("gdrive", StreamURLHandler(GoogleDriveFSClient))

# Directory Scanners

SCANNER_REGISTRY.register("", ClientDirScanner(LocalFSClient))
SCANNER_REGISTRY.register("file", ClientDirScanner(LocalFSClient))

SCANNER_REGISTRY.register("s3", ClientDirScanner(S3Client))

SCANNER_REGISTRY.register("ftp", ClientDirScanner(FTPClient))
SCANNER_REGISTRY.register("sftp", ClientDirScanner(SFTPClient))

SCANNER_REGISTRY.register("googledrive", ClientDirScanner(GoogleDriveFSClient))
SCANNER_REGISTRY.register("gdrive", ClientDirScanner(GoogleDriveFSClient))

# Db registry
DB_REGISTRY.register("postgresql", PostgresClient)
DB_REGISTRY.register("awsathena+rest", AthenaClient)

COPIER_REGISTRY.register("s3+s3", S3Client("s3://"))

REMOVER_REGISTRY.register("", ClientRemover(LocalFSClient))
REMOVER_REGISTRY.register("file", ClientRemover(LocalFSClient))
REMOVER_REGISTRY.register("s3", ClientRemover(S3Client))
REMOVER_REGISTRY.register("gs", ClientRemover(GSClient))
REMOVER_REGISTRY.register("gcs", ClientRemover(GSClient))
REMOVER_REGISTRY.register("ftp", ClientRemover(FTPClient))
REMOVER_REGISTRY.register("sftp", ClientRemover(SFTPClient))

REMOVER_REGISTRY.register("googledrive", ClientRemover(GoogleDriveFSClient))
REMOVER_REGISTRY.register("gdrive", ClientRemover(GoogleDriveFSClient))
