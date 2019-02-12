import functools
from typing import Callable, TypeVar

from dataio.clients import exceptions


T = TypeVar("T")


def check_conn(func: Callable[..., T]) -> Callable[..., T]:
    @functools.wraps(func)
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
