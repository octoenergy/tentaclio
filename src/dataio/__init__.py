from dataio.handlers import (
    S3URLHandler,
    LocalFileHandler,
)  # noqa <- automatic registration of default handlers
from .protocols import *  # noqa
from .urls import *  # noqa

# Local files
URL.register_handler("", LocalFileHandler())
URL.register_handler("file", LocalFileHandler())

# s3 buckets
URL.register_handler("s3", S3URLHandler())
