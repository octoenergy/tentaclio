import io

from tentaclio.fs.copier import CopierRegistry


def test_copy(mocker):
    mocked_open = mocker.patch("tentaclio.fs.copiers.open")
    orig = io.StringIO("contents")
    orig.seek(0)
    dest = io.StringIO("")
    mocked_open.return_value.__enter__.side_effect = [orig, dest]
    CopierRegistry().get_handler("file+file").copy(orig, dest)
    dest.seek(0)
    assert dest.getvalue() == "contents"
