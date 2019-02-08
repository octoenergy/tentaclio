import pytest

from dataio import URL
from dataio.api import _open_reader, _open_writer


athena_url = (
    "awsathena+rest://user:pw@athena.eu-west-1.amazonaws.com:443/default?s3_staging_dir=my_dir"
)


def test_athena_scheme():
    url = URL(athena_url)  # would crash if no hander
    assert url.scheme == "awsathena+rest"


@pytest.mark.parametrize("mode", ["r", "w"])
def test_handler_raises_error(mode):
    with pytest.raises(NotImplementedError):
        _open_reader(athena_url)

    with pytest.raises(NotImplementedError):
        _open_writer(athena_url)
