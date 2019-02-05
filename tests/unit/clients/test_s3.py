from dataio.clients import exceptions, s3_client

import moto
import pytest


@pytest.fixture()
def mocked_conn(mocker):
    with mocker.patch.object(s3_client.S3Client, "connect", return_value=mocker.Mock()):
        yield


@pytest.fixture()
def fixture_conn():
    with moto.mock_s3():
        yield


class TestS3Client:
    def test_invalid_scheme(self, register_handler):
        with pytest.raises(exceptions.S3Error):
            s3_client.S3Client("registered://file")

    @pytest.mark.parametrize(
        "url,username,password,hostname,path",
        [
            ("s3://:@s3", None, None, None, ""),
            ("s3://public_key:private_key@s3", "public_key", "private_key", None, ""),
            ("s3://:@bucket", None, None, "bucket", ""),
            ("s3://:@bucket/prefix", None, None, "bucket", "prefix"),
        ],
    )
    def test_parsing_s3_url(self, url, username, password, hostname, path):
        parsed_url = s3_client.S3Client(url).url

        assert parsed_url.scheme == "s3"
        assert parsed_url.hostname == hostname
        assert parsed_url.username == username
        assert parsed_url.password == password
        assert parsed_url.port is None
        assert parsed_url.path == path

    @pytest.mark.parametrize(
        "url,bucket,key",
        [
            ("s3://:@s3", None, None),
            ("s3://:@s3", "bucket", None),
            ("s3://:@bucket", None, None),
        ],
    )
    def test_get_invalid_path(self, url, bucket, key, mocked_conn):
        with s3_client.S3Client(url) as client:

            with pytest.raises(exceptions.S3Error):
                client.get(bucket_name=bucket, key_name=key)
