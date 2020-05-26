"""Test of the GS Client."""
import io

import pytest
import mock

from google.cloud import exceptions as google_exceptions

from tentaclio.clients import gs_client, exceptions


@mock.patch("tentaclio.clients.GSClient._connect")
@pytest.mark.parametrize(
    "url,hostname,path",
    [
        ("gs://bucket/prefix", "bucket", "prefix"),
        ("gs://:@gs", None, ""),
        ("gs://public_key:private_key@gs", None, ""),
        ("gs://:@bucket", "bucket", ""),
        ("gs://:@bucket/prefix", "bucket", "prefix"),
    ],
)
def test_parsing_gs_url(m_connect, url, hostname, path):
    """Test the parsing of the gs url."""
    client = gs_client.GSClient(url)

    assert client.key_name == path
    assert client.bucket == hostname


@mock.patch("tentaclio.clients.GSClient._get")
@mock.patch("tentaclio.clients.GSClient._connect")
@pytest.mark.parametrize(
    "url,bucket,key", [
        ("gs://:@gs", None, None),
        ("gs://:@gs", "bucket", None),
        ("gs://:@bucket", None, None)
    ],
)
def test_get_invalid_path(m_connect, m_get, url, bucket, key):
    """Test get for with invalid paths."""
    with gs_client.GSClient(url) as client:
        with pytest.raises(exceptions.GSError):
            client.get(io.StringIO(), bucket_name=bucket, key_name=key)

    m_get.assert_not_called()


@mock.patch("tentaclio.clients.GSClient._get")
@mock.patch("tentaclio.clients.GSClient._connect")
@pytest.mark.parametrize(
    "url,bucket,key", [
        ("gs://:@bucket/not_found", "bucket", "not_found")
    ],
)
def test_get_not_found(m_connect, m_get, url, bucket, key):
    """That when the connection raises a NotFound an GSError is thrown."""
    m_get.side_effect = google_exceptions.NotFound("not found")
    stream = io.StringIO()
    with gs_client.GSClient(url) as client:
        with pytest.raises(exceptions.GSError):
            client.get(stream, bucket_name=bucket, key_name=key)

    m_get.assert_called_once_with(stream, bucket, key)


@mock.patch("tentaclio.clients.GSClient._get")
@mock.patch("tentaclio.clients.GSClient._connect")
@pytest.mark.parametrize(
    "url,bucket,key", [
        ("gs://bucket/prefix", "bucket", "prefix"),
    ]
)
def test_get(m_connect, m_get, url, bucket, key):
    """Test get valid."""
    stream = io.StringIO()
    with gs_client.GSClient(url) as client:
        client.get(stream, bucket_name=bucket, key_name=key)

    m_get.assert_called_once_with(stream, bucket, key)


@mock.patch("tentaclio.clients.GSClient._connect")
@pytest.mark.parametrize(
    "url,bucket,key", [
        ("gs://bucket/prefix", "bucket", "prefix"),
    ]
)
def test_helper_get(m_connect, url, bucket, key):
    """Test helper get is correctly called."""
    stream = io.StringIO()
    with gs_client.GSClient(url) as client:
        client._get(stream, bucket_name=bucket, key_name=key)

    m_connect.return_value.bucket.assert_called_once_with(bucket)
