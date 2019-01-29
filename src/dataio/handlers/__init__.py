from dataio.url import URL
from .s3_handler import *  # noqa

URL.register_handler("s3", S3URLHandler())
