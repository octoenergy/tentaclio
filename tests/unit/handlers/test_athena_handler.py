import pytest

from dataio import URL, api


athena_url = (
    "awsathena+rest://user:pw@athena.eu-west-1.amazonaws.com:443/default?s3_staging_dir=my_dir"
)


def test_athena_scheme():
    url = URL(athena_url)  # would crash if no hander
    assert url.scheme == "awsathena+rest"


def test_handler_raises_error():
    with pytest.raises(NotImplementedError):
        api.open(athena_url)

    with pytest.raises(NotImplementedError):
        api.open(athena_url, mode="w")
