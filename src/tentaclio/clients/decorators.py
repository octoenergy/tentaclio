"""Decorators for internal use in clients module."""
import functools
from typing import Callable, TypeVar


T = TypeVar("T")


def check_conn(func: Callable[..., T]) -> Callable[..., T]:  # noqa
    """Check that the connection to client is done otherwise raise an exception."""

    @functools.wraps(func)
    def _wrapper(*args, **kwargs) -> T:
        # Instance is passed as first positional argument
        inst = args[0]

        if hasattr(inst, "closed"):
            if inst.closed:
                raise ConnectionError("The connection is closed")
        else:
            raise AttributeError("Missing instance closed attribute")

        return func(*args, **kwargs)

    return _wrapper
