import io

from dataio.clients import exceptions, ftp_client

import pytest


@pytest.fixture()
def mocked_ftp_conn(mocker):
    with mocker.patch.object(ftp_client.FTPClient, "connect", return_value=mocker.Mock()):
        yield


@pytest.fixture()
def mocked_sftp_conn(mocker):
    with mocker.patch.object(ftp_client.SFTPClient, "connect", return_value=mocker.Mock()):
        yield


class TestFTPClient:
    @pytest.mark.parametrize("url", ["file:///test.file", "sftp://:@localhost", "s3://:@s3"])
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

    def test_get(self, mocker):
        expected = bytes("hello", "utf-8")
        # the object from the fixture makes the mocking really ugly
        sftp_mock = mocker.patch("ftplib.FTP")
        sftp_mock.return_value.retrbinary = lambda _, f: f(expected)
        with ftp_client.FTPClient("ftp://user:pass@localhost/myfile.txt") as client:
            contents = client.get().read()
        assert contents == expected

    def test_put(self, mocker):
        expected = bytes("hello", "utf-8")
        # the object from the fixture makes the mocking really ugly
        sftp_mock = mocker.patch("ftplib.FTP")
        result = io.BytesIO()
        sftp_mock.return_value.storbinary = lambda _, f: result.write(f.read())
        with ftp_client.FTPClient("ftp://user:pass@localhost/myfile.txt") as client:
            reader = io.BytesIO(expected)
            client.put(reader)
        result.seek(0)
        assert result.getvalue() == expected


class TestSFTPClient:
    @pytest.mark.parametrize("url", ["file:///test.file", "ftp://:@localhost", "s3://:@s3"])
    def test_invalid_scheme(self, url):
        with pytest.raises(exceptions.FTPError):
            ftp_client.SFTPClient(url)

    @pytest.mark.parametrize("url,path", [("sftp://:@localhost", None)])
    def test_get_invalid_path(self, url, path, mocked_sftp_conn):
        with ftp_client.SFTPClient(url) as client:

            with pytest.raises(exceptions.FTPError):
                client.get(file_path=path)

    def test_get(self, mocker):
        expected = bytes("hello", "utf-8")
        # the object from the fixture makes the mocking really ugly
        sftp_mock = mocker.patch("pysftp.Connection")
        sftp_mock.return_value.getfo = lambda _, f: f.write(expected)
        with ftp_client.SFTPClient("sftp://user:pass@localhost/myfile.txt") as client:
            contents = client.get().read()
        assert contents == expected

    def test_put(self, mocker):
        expected = bytes("hello", "utf-8")
        # the object from the fixture makes the mocking really ugly
        sftp_mock = mocker.patch("pysftp.Connection")
        result = io.BytesIO()
        sftp_mock.return_value.putfo = lambda _, f: result.write(f.read())
        with ftp_client.SFTPClient("sftp://user:pass@localhost/myfile.txt") as client:
            reader = io.BytesIO(expected)
            client.put(reader)
        result.seek(0)
        assert result.getvalue() == expected
