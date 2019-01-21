import pytest
from dataio.clients import exceptions, s3_client


class TestS3Client:
    @pytest.mark.parametrize(
        "url", ["file:///test.file", "ftp://:@localhost", "postgresql://:@localhost"]
    )
    def test_invalid_scheme(self, url):
        with pytest.raises(exceptions.S3Error):
            s3_client.S3Client(url)

    @pytest.mark.parametrize(
        "url,username,password,hostname,path",
        [
            ("s3://:@s3", "", "", None, ""),
            ("s3://public_key:private_key@s3", "public_key", "private_key", None, ""),
            ("s3://:@bucket_name", "", "", "bucket_name", ""),
            ("s3://:@bucket_name/key", "", "", "bucket_name", "key"),
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
