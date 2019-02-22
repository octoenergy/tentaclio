import functools
from typing import Callable, TypeVar

from dataio.clients import exceptions


T = TypeVar("T")


def check_conn(func: Callable[..., T]) -> Callable[..., T]:
    @functools.wraps(func)
    def _wrapper(*args, **kwargs) -> T:
        # Instance is passed as first positional argument
        inst = args[0]

        if hasattr(inst, "closed"):
            if inst.closed:
                raise exceptions.ConnectionError("The connection is closed")
        else:
            raise AttributeError("Missing instance closed attribute")

        return func(*args, **kwargs)

    return _wrapper
