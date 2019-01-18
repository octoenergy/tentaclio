# Sanity checks on protocols
# These are not unit test but a check that the typing system works
# for the expected inputs.
from dataio import Reader, Writer

import pytest


class MyReader(object):

    def read(self, i=-1):
        pass


class MyWriter(object):

    def write(self, contents):
        pass


@pytest.mark.skip("Checked by mypy")
def test_reader():
    def func(reader: Reader):
        pass
    func(MyReader())


@pytest.mark.skip("Checked by mypy")
def test_writer():
    def func(writer: Writer):
        pass
    func(MyWriter())
