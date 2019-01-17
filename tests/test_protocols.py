# Sanity checks on protocols
import pytest

from dataio import Reader, Writer


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
