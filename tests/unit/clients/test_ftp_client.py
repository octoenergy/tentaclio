import io

import pytest

from tentaclio.clients import exceptions, ftp_client


@pytest.fixture()
def mocked_ftp_conn(mocker):
    with mocker.patch.object(ftp_client.FTPClient, "_connect", return_value=mocker.MagicMock()):
        yield


@pytest.fixture()
def mocked_sftp_conn(mocker):
    with mocker.patch.object(ftp_client.SFTPClient, "_connect", return_value=mocker.MagicMock()):
        yield


class TestFTPClient:
    @pytest.mark.parametrize("url", ["file:///test.file", "sftp://:@localhost", "s3://:@s3"])
    def test_invalid_scheme(self, url):
        with pytest.raises(ValueError):
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
                client.get(io.StringIO(), file_path=path)

    def test_get(self, mocked_ftp_conn):
        expected = bytes("hello", "utf-8")

        client = ftp_client.FTPClient("ftp://user:pass@localhost/myfile.txt")
        client.connect().retrbinary = lambda _, f: f(expected)
        buff = io.BytesIO()
        with client:
            client.get(buff)

        assert buff.getvalue() == expected

    def test_put(self, mocked_ftp_conn):
        expected = bytes("hello", "utf-8")
        result = io.BytesIO()

        client = ftp_client.FTPClient("ftp://user:pass@localhost/myfile.txt")
        client.connect().storbinary = lambda _, f: result.write(f.read())

        with client:
            reader = io.BytesIO(expected)
            client.put(reader)
        result.seek(0)

        assert result.getvalue() == expected


class TestSFTPClient:
    @pytest.mark.parametrize("url", ["file:///test.file", "ftp://:@localhost", "s3://:@s3"])
    def test_invalid_scheme(self, url):
        with pytest.raises(ValueError):
            ftp_client.SFTPClient(url)

    @pytest.mark.parametrize("url,path", [("sftp://:@localhost", None)])
    def test_get_invalid_path(self, url, path, mocked_sftp_conn):
        with ftp_client.SFTPClient(url) as client:

            with pytest.raises(exceptions.FTPError):
                client.get(io.StringIO(), file_path=path)

    def test_get(self, mocked_sftp_conn):
        expected = bytes("hello", "utf-8")

        client = ftp_client.SFTPClient("sftp://user:pass@localhost/myfile.txt")
        client.connect().getfo = lambda _, f: f.write(expected)
        buff = io.BytesIO()
        with client:
            client.get(buff)

        assert buff.getvalue() == expected

    def test_put(self, mocked_sftp_conn):
        expected = bytes("hello", "utf-8")
        result = io.BytesIO()

        client = ftp_client.SFTPClient("sftp://user:pass@localhost/myfile.txt")
        # forgive me father for I have sinned.
        # mocking the context manager of the connection, a bit coupled with the
        # actual implementation and long.
        client.connect().open().__enter__().write = lambda data: result.write(data)

        with client:
            reader = io.BytesIO(expected)
            client.put(reader)
        result.seek(0)

        assert result.getvalue() == expected

    @pytest.mark.parametrize(
        ["url", "port"],
        [
            ("sftp://user:pass@localhost/myfile.txt", 22),
            ("sftp://user:pass@localhost:200/myfile.txt", 200),
        ],
    )
    def test_set_default_port(self, url, port, mocked_sftp_conn):
        client = ftp_client.SFTPClient(url)
        assert client.port == port
