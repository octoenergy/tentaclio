import moto
import pytest

from dataio.clients import exceptions, s3_client


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
        client = s3_client.S3Client(url)

        assert client.aws_access_key_id == username
        assert client.aws_secret_access_key == password
        assert client.key_name == path
        assert client.bucket == hostname

    @pytest.mark.parametrize(
        "url,bucket,key",
        [("s3://:@s3", None, None), ("s3://:@s3", "bucket", None), ("s3://:@bucket", None, None)],
    )
    def test_get_invalid_path(self, url, bucket, key, mocked_conn):
        with s3_client.S3Client(url) as client:

            with pytest.raises(exceptions.S3Error):
                client.get(bucket_name=bucket, key_name=key)
