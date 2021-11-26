"""Client exceptions."""


class ClientError(Exception):
    """Basic exception raised for a given product."""


class FTPError(ClientError):
    """Exception encountered over a FTP client connection."""


class S3Error(ClientError):
    """Exception encountered over a S3 client connection."""


class GSError(ClientError):
    """Exception encountered over a GS client connection."""


class HTTPError(ClientError):
    """Exception encountered over a HTTP client connection."""
