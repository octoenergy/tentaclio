class ClientError(Exception):
    """
    Basic exception raised for a given product
    """


class ConnectionError(ClientError):
    """
    Exception encountered while calling a client connection
    """


class PostgresError(Exception):
    """
    Exception encountered over a Postgres client connection
    """


class FTPError(Exception):
    """
    Exception encountered over a FTP client connection
    """


class S3Error(Exception):
    """
    Exception encountered over a S3 client connection
    """


class HTTPError(Exception):
    """
    Exception encountered over a HTTP client connection
    """
