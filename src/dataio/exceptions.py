class ClientError(Exception):
    """
    Basic exception raised for a given product
    """


class ConnError(ClientError):
    """
    Exception encountered while calling a client connection
    """


class FTPError(ClientError):
    """
    Exception encountered over a FTP client connection
    """


class S3Error(ClientError):
    """
    Exception encountered over a S3 client connection
    """


class HTTPError(ClientError):
    """
    Exception encountered over a HTTP client connection
    """


class URLError(Exception):
    """
    Error encountered while processing a URL
    """
