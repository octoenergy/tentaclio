import io
import os
import tempfile

import pytest

from tentaclio import URL, api


@pytest.fixture
def temp_filename():
    with tempfile.NamedTemporaryFile() as f:
        name = f.name
    yield name
    try:
        os.remove(name)
    except Exception:
        pass


def test_file_empty_scheme():
    url = URL("/path/to/my/file")  # would crash if no hander
    assert url.path == "/path/to/my/file"


def test_file_scheme():
    url = URL("file:///path/to/my/file")  # would crash if no hander
    assert url.path == "/path/to/my/file"


@pytest.mark.parametrize(
    ["mode", "expected"],
    [("", "hello file"), ("t", "hello file"), ("b", bytes("hello file", "utf-8"))],
)
def test_file_read_write(mode, expected, temp_filename):
    with api.open(temp_filename, mode="w" + mode) as writer:
        contents = writer.write(expected)

    with api.open(temp_filename, mode=mode) as reader:
        contents = reader.read()

    assert contents == expected


@pytest.mark.parametrize(["mode"], [("r"), ("b")])
def test_expand_user(mode, mocker):
    file_name = "~"
    mocked_open = mocker.patch("builtins.open")
    opened_path = io.StringIO()

    mocked_open.side_effect = lambda path, mode: opened_path.write(path)

    # just to check the value used in open, and not having
    # to create a full context manager
    api.open(file_name, mode=mode).__enter__()

    assert opened_path.getvalue() == os.path.expanduser(file_name)
