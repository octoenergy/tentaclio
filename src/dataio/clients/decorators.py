from typing import Callable, TypeVar

from dataio.clients import exceptions

T = TypeVar("T")


class check_conn:
    """
    Decorator for testing the status of a client connection
    """

    def __init__(self, *args, **kwargs) -> None:
        pass

    def __call__(self, func: Callable) -> Callable:
        def _wrapper(*args, **kwargs) -> T:
            # Instance is passed as first positional argument
            inst = args[0]

            if hasattr(inst, "conn"):
                if inst.conn is None:
                    raise exceptions.ConnectionError("Inactive client connection")
            else:
                raise AttributeError("Missing instance connection attribute")

            return func(*args, **kwargs)

        return _wrapper
