import pytest
from dataio.hooks import base_hook


class TestCredential:

    # Generic parsing rules:

    def test_missing_uri(self):
        with pytest.raises(ValueError):
            base_hook.Credential()

    @pytest.mark.parametrize(
        "uri",
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
    def test_unknown_uri_scheme(self, uri):
        with pytest.raises(AssertionError):
            base_hook.Credential(uri=uri)

    # File URI:

    @pytest.mark.parametrize(
        "uri,path",
        [("file:///test.file", "/test.file"), ("file:///dir/test.file", "/dir/test.file")],
    )
    def test_parsing_file_uri(self, uri, path):
        cred = base_hook.Credential(uri=uri)

        assert cred.scheme == "file"
        assert cred.hostname is None
        assert cred.username is None
        assert cred.password is None
        assert cred.port is None
        assert cred.path == path

    # S3 URI:

    @pytest.mark.parametrize(
        "uri,username,password,hostname,path",
        [
            ("s3://:@s3", "", "", None, ""),
            ("s3://public_key:private_key@s3", "public_key", "private_key", None, ""),
            ("s3://:@bucket_name", "", "", "bucket_name", ""),
            ("s3://:@bucket_name/key", "", "", "bucket_name", "key"),
        ],
    )
    def test_parsing_s3_uri(self, uri, username, password, hostname, path):
        cred = base_hook.Credential(uri=uri)

        assert cred.scheme == "s3"
        assert cred.hostname == hostname
        assert cred.username == username
        assert cred.password == password
        assert cred.port is None
        assert cred.path == path

    # Postgres URI:

    @pytest.mark.parametrize(
        "uri,username,password,hostname,port,path",
        [
            ("postgres://:@localhost", "", "", "localhost", None, ""),
            ("postgres://login:pass@localhost", "login", "pass", "localhost", None, ""),
            ("postgres://:@localhost:5432", "", "", "localhost", 5432, ""),
            ("postgres://:@localhost:5432/database", "", "", "localhost", 5432, "database"),
        ],
    )
    def test_postgres_uri(self, uri, username, password, hostname, port, path):
        cred = base_hook.Credential(uri=uri)

        assert cred.scheme == "postgres"
        assert cred.hostname == hostname
        assert cred.username == username
        assert cred.password == password
        assert cred.port == port
        assert cred.path == path
