import pytest
from dataio.clients import exceptions, ftp_client


@pytest.fixture()
def mocked_ftp_conn(mocker):
    with mocker.patch.object(
        ftp_client.FTPClient, "connect", return_value=mocker.Mock()
    ):
        yield


@pytest.fixture()
def mocked_sftp_conn(mocker):
    with mocker.patch.object(
        ftp_client.SFTPClient, "connect", return_value=mocker.Mock()
    ):
        yield


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
    def test_parsing_ftp_url(self, url, username, password, hostname, port, path):
        parsed_url = ftp_client.FTPClient(url).url

        assert parsed_url.scheme == "ftp"
        assert parsed_url.hostname == hostname
        assert parsed_url.username == username
        assert parsed_url.password == password
        assert parsed_url.port == port
        assert parsed_url.path == path

    @pytest.mark.parametrize("url,path", [("ftp://:@localhost", None)])
    def test_get_invalid_path(self, url, path, mocked_ftp_conn):
        with ftp_client.FTPClient(url) as client:

            with pytest.raises(exceptions.FTPError):
                client.get(file_path=path)


class TestSFTPClient:
    @pytest.mark.parametrize(
        "url", ["file:///test.file", "ftp://:@localhost", "s3://:@s3"]
    )
    def test_invalid_scheme(self, url):
        with pytest.raises(exceptions.FTPError):
            ftp_client.SFTPClient(url)

    @pytest.mark.parametrize("url,path", [("sftp://:@localhost", None)])
    def test_get_invalid_path(self, url, path, mocked_sftp_conn):
        with ftp_client.SFTPClient(url) as client:

            with pytest.raises(exceptions.FTPError):
                client.get(file_path=path)

    def test_get(self,mocked_sftp_conn):
        pass

