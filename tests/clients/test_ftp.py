import pytest
from dataio.clients import exceptions, ftp_client


class TestFTPClient:
    @pytest.mark.parametrize(
        "url", ["file:///test.file", "sftp://:@localhost", "s3://:@s3"]
    )
    def test_invalid_scheme(self, url):
        with pytest.raises(exceptions.FTPError):
            ftp_client.FTPClient(url)

    @pytest.mark.parametrize(
        "url,username,password,hostname,port,path",
        [
            ("ftp://:@localhost", "", "", "localhost", None, ""),
            ("ftp://login:pass@localhost", "login", "pass", "localhost", None, ""),
            ("ftp://:@localhost:21", "", "", "localhost", 21, ""),
            ("ftp://:@localhost:21/path", "", "", "localhost", 21, "/path"),
        ],
    )
    def test_ftp_url(self, url, username, password, hostname, port, path):
        parsed_url = ftp_client.FTPClient(url).url

        assert parsed_url.scheme == "ftp"
        assert parsed_url.hostname == hostname
        assert parsed_url.username == username
        assert parsed_url.password == password
        assert parsed_url.port == port
        assert parsed_url.path == path


class TestSFTPClient:
    @pytest.mark.parametrize(
        "url", ["file:///test.file", "ftp://:@localhost", "s3://:@s3"]
    )
    def test_invalid_scheme(self, url):
        with pytest.raises(exceptions.FTPError):
            ftp_client.SFTPClient(url)
