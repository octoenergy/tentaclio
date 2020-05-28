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


def patch_string_from_method(method: str) -> str:
    """Get the string that indicates the method to be patched for a calling method."""
    switch_dict = {
        "get": "tentaclio.clients.GSClient._get",
        "put": "tentaclio.clients.GSClient._put"
    }

    if method in switch_dict:
        return switch_dict[method]

    raise Exception(f"No patch string for calling method: {method}")


@mock.patch("tentaclio.clients.GSClient._connect")
@pytest.mark.parametrize(
    "method,url,bucket,key", [
        ("get", "gs://:@gs", None, None),
        ("get", "gs://:@gs", "bucket", None),
        ("get", "gs://:@bucket", None, None),
        ("put", "gs://:@gs", None, None),
        ("put", "gs://:@gs", "bucket", None),
        ("put", "gs://:@bucket", None, None)
    ],
)
def test_invalid_path(m_connect, method, url, bucket, key):
    """Test a method with invalid paths."""
    patch_string = patch_string_from_method(method)

    with mock.patch(patch_string) as mocked_method:
        with gs_client.GSClient(url) as client:
            with pytest.raises(exceptions.GSError):
                calling_method = getattr(client, method)
                calling_method(io.StringIO(), bucket_name=bucket, key_name=key)

        mocked_method.assert_not_called()


@mock.patch("tentaclio.clients.GSClient._connect")
@pytest.mark.parametrize(
    "method,url,bucket,key", [
        ("get", "gs://:@bucket/not_found", "bucket", "not_found"),
        ("put", "gs://:@bucket/not_found", "bucket", "not_found")
    ],
)
def test_not_found(m_connect, method, url, bucket, key):
    """That when the connection raises a NotFound it is raised."""
    patch_string = patch_string_from_method(method)

    with mock.patch(patch_string) as mocked_method:
        mocked_method.side_effect = google_exceptions.NotFound("not found")
        stream = io.StringIO()
        with gs_client.GSClient(url) as client:
            with pytest.raises(google_exceptions.NotFound):
                calling_method = getattr(client, method)
                calling_method(stream, bucket_name=bucket, key_name=key)
        mocked_method.assert_called_once_with(stream, bucket, key)


@mock.patch("tentaclio.clients.GSClient._connect")
@pytest.mark.parametrize(
    "method,url,bucket,key", [
        ("get", "gs://bucket/prefix", "bucket", "prefix"),
        ("put", "gs://bucket/prefix", "bucket", "prefix")
    ]
)
def test_method(m_connect, method, url, bucket, key):
    """Test method with valid call."""
    patch_string = patch_string_from_method(method)

    with mock.patch(patch_string) as mocked_method:
        stream = io.StringIO()
        with gs_client.GSClient(url) as client:
            calling_method = getattr(client, method)
            calling_method(stream, bucket_name=bucket, key_name=key)
        mocked_method.assert_called_once_with(stream, bucket, key)


@mock.patch("tentaclio.clients.GSClient._connect")
@pytest.mark.parametrize(
    "url,bucket,key", [
        ("gs://bucket/prefix", "bucket", "prefix"),
    ]
)
def test_helper_get(m_connect, url, bucket, key):
    """Test helper _get is correctly called."""
    stream = io.StringIO()
    with gs_client.GSClient(url) as client:
        client._get(stream, bucket_name=bucket, key_name=key)

    connection = m_connect.return_value
    connection.bucket.assert_called_once_with(bucket)
    connection.bucket.return_value.blob.assert_called_once_with(key)
    connection.bucket.return_value.blob.return_value.download_to_file.assert_called_once()


@mock.patch("tentaclio.clients.GSClient._connect")
@pytest.mark.parametrize(
    "url,bucket,key", [
        ("gs://bucket/prefix", "bucket", "prefix"),
    ]
)
def test_helper_put(m_connect, url, bucket, key):
    """Test helper _put is correctly called."""
    stream = io.StringIO()
    with gs_client.GSClient(url) as client:
        client._put(stream, bucket_name=bucket, key_name=key)

    connection = m_connect.return_value
    connection.bucket.assert_called_once_with(bucket)
    connection.bucket.return_value.blob.assert_called_once_with(key)
    connection.bucket.return_value.blob.return_value.upload_from_file.assert_called_once()
