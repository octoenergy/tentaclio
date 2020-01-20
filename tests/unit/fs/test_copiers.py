import io

from tentaclio.fs import DefaultCopier


def test_copy(mocker):
    mocked_open = mocker.patch("tentaclio.fs.copiers.open")
    orig = io.StringIO("contents")
    orig.seek(0)
    dest = io.StringIO("")
    mocked_open.return_value.__enter__.side_effect = [orig, dest]
    DefaultCopier().copy(orig, dest)
    dest.seek(0)
    assert dest.getvalue() == "contents"
