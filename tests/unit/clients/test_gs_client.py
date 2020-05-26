"""Test of the GS Client."""

import pytest
import mock

from tentaclio.clients import gs_client


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
