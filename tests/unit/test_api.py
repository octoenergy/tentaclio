import pytest

from dataio.api import _assert_mode


@pytest.mark.parametrize("mode", ["r", "w", "b", "t", "rb", "rt", "wt", "wb", ""])
def test_allowed_modes(mode):
    _assert_mode(mode)  # exception raised if not allowed


def test_mode_error():
    with pytest.raises(ValueError):
        _assert_mode("jj")
