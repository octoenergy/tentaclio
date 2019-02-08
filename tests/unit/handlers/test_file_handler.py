import os
import tempfile

import pytest

from dataio import URL
from dataio.api import _open_reader, _open_writer


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
    ["extras", "expected"],
    [
        ({}, "hello file"),
        ({"mode": "t"}, "hello file"),
        ({"mode": "rt"}, "hello file"),
        ({"mode": "wt"}, "hello file"),
        ({"mode": "b"}, bytes("hello file", "utf-8")),
        ({"mode": "wb"}, bytes("hello file", "utf-8")),
        ({"mode": "rb"}, bytes("hello file", "utf-8")),
    ],
)
def test_file_read_write(extras, expected, temp_filename):
    with _open_writer(temp_filename, **extras) as writer:
        contents = writer.write(expected)

    with _open_reader(temp_filename, **extras) as reader:
        contents = reader.read()

    assert contents == expected
