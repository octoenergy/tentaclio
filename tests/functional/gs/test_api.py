"""Test the api."""
import pytest

import tentaclio

from google.cloud import exceptions as google_exceptions


def test_authenticated_api_calls(gs_url, fixture_client):
    """Test the authetnciated api calls."""
    data = bytes("Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn", "utf-8")

    with tentaclio.open(gs_url, mode="wb") as f:
        f.write(data)

    with tentaclio.open(gs_url, mode="rb") as f:
        result = f.read()

    assert result == data

    tentaclio.remove(gs_url)

    with pytest.raises(google_exceptions.NotFound):
        with tentaclio.open(gs_url, mode="rb") as f:
            result = f.read()
