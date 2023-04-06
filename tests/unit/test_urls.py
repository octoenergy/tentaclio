import pytest

from tentaclio import urls


class TestURL:
    # Generic parsing rules:
    def test_missing_url(self):
        with pytest.raises(urls.URLError):
            urls.URL(None)

    @pytest.mark.parametrize(
        "url,username,password",
        [
            ("scheme://:@localhost", "", ""),
            ("scheme://abc_def%40123.com:@localhost", "abc_def@123.com", ""),
            ("scheme://:%40%60%60Z_%24-%24%405%25Ky%2F@localhost", "", "@``Z_$-$@5%Ky/"),
        ],
    )
    def test_url_escaped_fields(self, url, username, password):
        parsed_url = urls.URL(url)

        assert parsed_url.username == username
        assert parsed_url.password == password

    @pytest.mark.parametrize(
        "url,path",
        [("file:///test.file", "/test.file"), ("file:///dir/test.file", "/dir/test.file")],
    )
    def test_parsing_file_url(self, url, path):
        parsed_url = urls.URL(url)

        assert parsed_url.scheme == "file"
        assert parsed_url.hostname is None
        assert parsed_url.username is None
        assert parsed_url.password is None
        assert parsed_url.port is None
        assert parsed_url.path == path

    @pytest.mark.parametrize(
        "url,scheme,username,password,hostname,port,path,query",
        [
            (
                "scheme:/path/to/file.ext",
                "scheme",
                None,
                None,
                None,
                None,
                "/path/to/file.ext",
                None,
            ),
            (
                "scheme://login:pass@localhost?key=value",
                "scheme",
                "login",
                "pass",
                "localhost",
                None,
                "",
                {"key": "value"},
            ),
            (
                "scheme://:@localhost:5432/database",
                "scheme",
                "",
                "",
                "localhost",
                5432,
                "/database",
                {},
            ),
            (
                "scheme://log%3Ain:pass%2Fword@localhost/path/to?spaced+key=odd%2Fva%3B%3Alue",
                "scheme",
                "log:in",
                "pass/word",
                "localhost",
                None,
                "/path/to",
                {"spaced key": "odd/va;:lue"},
            ),
        ],
    )
    def test_url_from_components(
        self, url, scheme, username, password, hostname, port, path, query
    ):
        parsed_url = urls.URL.from_components(
            scheme=scheme,
            username=username,
            password=password,
            hostname=hostname,
            port=port,
            path=path,
            query=query,
        )
        assert parsed_url.url == url
        assert parsed_url.scheme == scheme
        assert parsed_url.username == username
        assert parsed_url.password == password
        assert parsed_url.hostname == hostname
        assert parsed_url.port == port
        assert parsed_url.path == path
        assert (parsed_url.query == query) or (not query and parsed_url.query is None)

    @pytest.mark.parametrize(
        ["url_1", "url_2", "should_be_equal"],
        [
            ["scheme://user:pass@host/path", "scheme://user:pass@host/path", True],
            ["scheme://host/path", "scheme://host/path", True],
            ["scheme://host/path?key=value", "scheme://host/path?key=value", True],
            ["scheme://host/path", "scheme://host/path?key=value", False],
            ["scheme://host/path", "scheme://user:pass@host/path", False],
            ["scheme://host/path1", "scheme://host/path2", False],
        ],
    )
    def test_url_equality(self, url_1, url_2, should_be_equal):
        assert (urls.URL(url_1) == urls.URL(url_2)) == should_be_equal

    @pytest.mark.parametrize(
        "original,components,expected",
        [
            (
                "scheme://user:password@hostname.com:7744/path?my_arg=my_value",
                {"username": "myuser"},
                "scheme://myuser:password@hostname.com:7744/path?my_arg=my_value",
            ),
            (
                "scheme://user:password@hostname.com:7744/path?my_arg=my_value",
                {"password": "mypassword"},
                "scheme://user:mypassword@hostname.com:7744/path?my_arg=my_value",
            ),
            (
                "scheme://user:password@hostname.com:7744/path?my_arg=my_value",
                {"hostname": "google.com"},
                "scheme://user:password@google.com:7744/path?my_arg=my_value",
            ),
            (
                "scheme://user:password@hostname.com:7744/path?my_arg=my_value",
                {"port": 80},
                "scheme://user:password@hostname.com:80/path?my_arg=my_value",
            ),
            (
                "scheme://user:password@hostname.com:7744/path?my_arg=my_value",
                {"path": "otherpath"},
                "scheme://user:password@hostname.com:7744/otherpath?my_arg=my_value",
            ),
            (
                "scheme://user:password@hostname.com:7744/path?my_arg=my_value",
                {"query": {"my_arg": "other_value"}},
                "scheme://user:password@hostname.com:7744/path?my_arg=other_value",
            ),
        ],
    )
    def test_copy(self, original, components, expected):
        result = urls.URL(original).copy(**components)
        assert result == urls.URL(expected)

    def test_string_hides_password(self):
        original = urls.URL("scheme://user:password@hostname.com")
        str_url = str(original)
        assert str_url == "scheme://user:__secret__word@hostname.com"
