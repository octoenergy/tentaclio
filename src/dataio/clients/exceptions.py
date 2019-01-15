class ClientError(Exception):
    """
    Basic exception raised for a given product
    """


class URIError(ClientError):
    """
    Exception encountered while processing a URI
    """


class ConnectionError(ClientError):
    """
    Exception encountered while calling a client connection
    """
