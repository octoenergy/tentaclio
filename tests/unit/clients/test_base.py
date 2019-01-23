import pytest
from dataio.clients import base_client, exceptions


class TestURL:

    # Generic parsing rules:

    def test_missing_url(self):
        with pytest.raises(exceptions.URIError):
            base_client.URL(None)

    @pytest.mark.parametrize(
        "url",
        [
            "http://localhost/path",
            "https://localhost:8888/path",
            "/path",
            "/path/fragment",
            "postgres://:@localhost:5432/path",
            "mysql://:@localhost:3306/path",
            "awss3://:@s3",
        ],
    )
    def test_unknown_url_scheme(self, url):
        with pytest.raises(exceptions.URIError):
            base_client.URL(url)

    @pytest.mark.parametrize(
        "url,username,password",
        [
            ("ftp://:@localhost", "", ""),
            ("ftp://abc_def%40123.com:@localhost", "abc_def@123.com", ""),
            ("ftp://:%40%60%60Z_%24-%24%405%25Ky%2F@localhost", "", "@``Z_$-$@5%Ky/"),
        ],
    )
    def test_url_escaped_fields(self, url, username, password):
        parsed_url = base_client.URL(url)

        assert parsed_url.username == username
        assert parsed_url.password == password

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

    # Test URL from parts
    @pytest.mark.parametrize("url,scheme,username,password,hostname,port,path,query", [
        ("file:///path/to",
            "file", None, None, None,
            None, "/path/to", None),
        ("postgresql://login:pass@localhost?key=value",
            "postgresql", "login", "pass", "localhost", None, "", {'key': 'value'}),
        ("postgresql://:@localhost:5432/database",
            "postgresql", "", "", "localhost", 5432, "/database", {}),
        ("postgresql://log%3Ain:pass%2Fword@localhost/path/to?spaced+key=odd%2Fva%3B%3Alue",
            "postgresql", "log:in", "pass/word", "localhost",
            None, "/path/to", {'spaced key': 'odd/va;:lue'}),
    ])
    def test_url_from_parts(self, url, scheme, username, password, hostname, port, path, query):
        parsed_url = base_client.URL.from_parts(
            scheme=scheme,
            username=username,
            password=password,
            hostname=hostname,
            port=port,
            path=path,
            query=query
        )
        assert parsed_url.url == url
        assert parsed_url.scheme == scheme
        assert parsed_url.username == username
        assert parsed_url.password == password
        assert parsed_url.hostname == hostname
        assert parsed_url.port == port
        assert parsed_url.path == path
        assert (parsed_url.query == query) or (not query and parsed_url.query is None)

class TestBaseClient:
    def test_client_url_scheme(self):
        url = "file:///path"

        class TestClient(base_client.BaseClient):
            def connect(self):
                return None

        test_client = TestClient(url)

        assert test_client.url.scheme == "file"

    def test_closed_client_connection(self, mocker):
        url = "file:///path"
        mocked_conn = mocker.Mock()

        class TestClient(base_client.BaseClient):
            def connect(self):
                return mocked_conn

        with TestClient(url) as test_client:
            pass

        mocked_conn.close.assert_called()
        assert test_client.conn is None
