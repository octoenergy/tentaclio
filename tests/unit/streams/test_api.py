import pytest

from tentaclio.streams import api


W_MODES = ["w", "wt", "wb"]
R_MODES = ["", "r", "b", "t", "rb", "rt"]


def test_mode_error():
    with pytest.raises(ValueError):
        api._assert_mode("jj")


@pytest.mark.parametrize("mode", W_MODES + R_MODES)
def test_allowed_modes(mode):
    api._assert_mode(mode)  # exception raised if not allowed


@pytest.mark.parametrize("mode", W_MODES)
def test_writer_modes(mode, mocker):
    mocked_open_writer = mocker.patch.object(api, "_open_writer")

    api.open("file://path/query", mode)

    mocked_open_writer.assert_called_once()


@pytest.mark.parametrize("mode", R_MODES)
def test_reader_modes(mode, mocker):
    mocked_open_reader = mocker.patch.object(api, "_open_reader")

    api.open("file://path/query", mode)

    mocked_open_reader.assert_called_once()


def test_make_empty_safe(mocker):
    writer = mocker.MagicMock()
    wrapped = api.make_empty_safe(writer)
    with wrapped as w:
        assert not w.dirty
