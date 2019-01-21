# Sanity checks on protocols
# These are not unit test but a check that the typing system works
# for the expected inputs.

import pytest

from dataio import protocols


class MyReader(object):
    def read(self, i=-1):
        pass


class MyWriter(object):
    def write(self, contents):
        pass


@pytest.mark.skip("Checked by mypy")
def test_reader():
    def func(reader: protocols.Reader):
        pass

    func(MyReader())


@pytest.mark.skip("Checked by mypy")
def test_writer():
    def func(writer: protocols.Writer):
        pass

    func(MyWriter())
