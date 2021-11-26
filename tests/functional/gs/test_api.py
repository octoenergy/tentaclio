"""Test the api."""
import pytest
import mock

import tentaclio
from . import conftest

from google.cloud import exceptions as google_exceptions


@pytest.mark.skipif(
    not conftest.GS_TEST_URL,
    reason="Not used in CI as authenticated environment is needed."
)
def test_authenticated_api_calls(gs_url, bucket_exists):
    """Test the authenticated API calls.

    Skipped unless configured in conftest or TENTACLIO__CONN__GS_TEST is set.

    🚨🚨🚨WARNING🚨🚨🚨
    The functional test for GS will:
        - create a bucket if non is found.
            This will be created in the configured project (see gcloud docs).
        - upload and remove a file as set in the URL

    To use run the test use command:
    ```
    env TENTACLIO__CONN__GS_TEST=gs://tentaclio-bucket/test.txt \
        make functional-gs
    ```

    You will need to have your environment configured, credentials and project.
    This is done with the gcloud cli tool. See docs for more information:
        https://googleapis.dev/python/google-api-core/latest/auth.html
    """
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


@mock.patch("tentaclio.clients.gs_client.GSClient._remove")
@mock.patch("tentaclio.clients.gs_client.GSClient._put")
@mock.patch("tentaclio.clients.gs_client.GSClient._get")
@mock.patch("tentaclio.clients.gs_client.GSClient._connect")
@pytest.mark.parametrize(
    "url,bucket,key", [
        ("gs://:@bucket/key", "bucket", "key"),
        ("gcs://:@bucket/key", "bucket", "key"),
    ],
)
def test_mocked_api_calls(m_connect, m_get, m_put, m_remove, url, bucket, key):
    """Test api calls reaches the mocks."""
    data = bytes("Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn", "utf-8")

    with tentaclio.open(url, mode="wb") as f:
        f.write(data)
    m_put.assert_called_once_with(mock.ANY, bucket, key)

    with tentaclio.open(url, mode="rb") as f:
        f.read()
    m_get.assert_called_once_with(mock.ANY, bucket, key)

    tentaclio.remove(url)
    m_remove.assert_called_once_with(bucket, key)

    m_get.reset_mock()
    m_get.side_effect = google_exceptions.NotFound("Not found")
    with pytest.raises(google_exceptions.NotFound):
        tentaclio.open(url, mode="rb")
