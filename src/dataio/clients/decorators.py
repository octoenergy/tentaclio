from typing import Any, Callable

from dataio.clients import exceptions


class check_conn:
    """
    Decorator for testing the status of a client connection
    """

    def __init__(self, *args, **kwargs) -> None:
        ...

    def __call__(self, func: Callable) -> Callable:
        def _wrapper(*args, **kwargs) -> Any:
            # Instance is passed as first positional argument
            inst = args[0]

            if hasattr(inst, "conn"):
                if inst.conn is None:
                    raise exceptions.ConnectionError("Inactive client connection")
            else:
                raise AttributeError("Missing instance connection attribute")

            return func(*args, **kwargs)

        return _wrapper
