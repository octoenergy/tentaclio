import pytest

from dataio.clients import base_client, exceptions


class TestCredential:

    # Generic parsing rules:

    def test_missing_url(self):
        with pytest.raises(exceptions.URIError):
            base_client.URL(None)

    @pytest.mark.parametrize(
        "url",
        [
            "ftp://:@localhost",
            "sftp://:@localhost",
            "http://localhost/path",
            "https://localhost:port/path",
            "/path",
            "/path/fragment",
            "postgresql://:@localhost:port",
            "pgsql://:@localhost:port/path",
            "awss3://:@s3",
        ],
    )
    def test_unknown_url_scheme(self, url):
        with pytest.raises(exceptions.URIError):
            base_client.URL(url)

    # File URL:

    @pytest.mark.parametrize(
        "url,path",
        [("file:///test.file", "/test.file"), ("file:///dir/test.file", "/dir/test.file")],
    )
    def test_parsing_file_url(self, url, path):
        parsed_url = base_client.URL(url)

        assert parsed_url.scheme == "file"
        assert parsed_url.hostname is None
        assert parsed_url.username is None
        assert parsed_url.password is None
        assert parsed_url.port is None
        assert parsed_url.path == path

    # S3 URL:

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
        parsed_url = base_client.URL(url)

        assert parsed_url.scheme == "s3"
        assert parsed_url.hostname == hostname
        assert parsed_url.username == username
        assert parsed_url.password == password
        assert parsed_url.port is None
        assert parsed_url.path == path

    # Postgres URL:

    @pytest.mark.parametrize(
        "url,username,password,hostname,port,path",
        [
            ("postgres://:@localhost", "", "", "localhost", None, ""),
            ("postgres://login:pass@localhost", "login", "pass", "localhost", None, ""),
            ("postgres://:@localhost:5432", "", "", "localhost", 5432, ""),
            ("postgres://:@localhost:5432/database", "", "", "localhost", 5432, "database"),
        ],
    )
    def test_postgres_url(self, url, username, password, hostname, port, path):
        parsed_url = base_client.URL(url)

        assert parsed_url.scheme == "postgres"
        assert parsed_url.hostname == hostname
        assert parsed_url.username == username
        assert parsed_url.password == password
        assert parsed_url.port == port
        assert parsed_url.path == path


class TestBaseClient:
    def test_client_url_scheme(self):
        url = "file:///path"

        class TestClient(base_client.BaseClient):
            def get_conn(self):
                return None

        test_client = TestClient(url)

        assert test_client.url.scheme == "file"

    def test_closed_client_connection(self, mocker):
        url = "file:///path"
        mocked_conn = mocker.Mock()

        class TestClient(base_client.BaseClient):
            def get_conn(self):
                return mocked_conn

        with TestClient(url) as test_client:
            pass

        mocked_conn.close.assert_called()
        assert test_client.conn is None
