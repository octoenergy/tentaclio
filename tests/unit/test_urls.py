import pytest

from dataio import urls


class TestRegistry(object):
    def test_register_handler(self, fake_handler):
        registry = urls.URLHandlerRegistry()
        registry.register("scheme", fake_handler)
        result = registry.get_handler("scheme")
        assert fake_handler is result

    def test_unknown_handler(self):
        registry = urls.URLHandlerRegistry()
        with pytest.raises(urls.URLError):
            registry.get_handler("scheme")

    def test_contains_handler(self, fake_handler):
        registry = urls.URLHandlerRegistry()
        registry.register("http", fake_handler)
        assert "http" in registry
        assert "https" not in registry


class TestURL:

    # Generic parsing rules:
    def test_missing_url(self):
        with pytest.raises(urls.URLError):
            urls.URL(None)

    def test_unknown_url_scheme(self):
        url = "notregistered://localhost"
        with pytest.raises(urls.URLError):
            urls.URL(url)

    def test_known_url_scheme(self, register_handler):
        url = "registered://localhost"
        # well, this shouldn't blow
        urls.URL(url)

    @pytest.mark.parametrize(
        "url,username,password",
        [
            ("registered://:@localhost", "", ""),
            ("registered://abc_def%40123.com:@localhost", "abc_def@123.com", ""),
            ("registered://:%40%60%60Z_%24-%24%405%25Ky%2F@localhost", "", "@``Z_$-$@5%Ky/"),
        ],
    )
    def test_url_escaped_fields(self, url, username, password, register_handler):
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
                "registered:///path/to/file.ext",
                "registered",
                None,
                None,
                None,
                None,
                "/path/to/file.ext",
                None,
            ),
            (
                "registered://login:pass@localhost?key=value",
                "registered",
                "login",
                "pass",
                "localhost",
                None,
                "",
                {"key": "value"},
            ),
            (
                "registered://:@localhost:5432/database",
                "registered",
                "",
                "",
                "localhost",
                5432,
                "/database",
                {},
            ),
            (
                "registered://log%3Ain:pass%2Fword@localhost/path/to?spaced+key=odd%2Fva%3B%3Alue",
                "registered",
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
        self, url, scheme, username, password, hostname, port, path, query, register_handler
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
            ["registered://user:pass@host/path", "registered://user:pass@host/path", True],
            ["registered://host/path", "registered://host/path", True],
            ["registered://host/path?key=value", "registered://host/path?key=value", True],
            ["registered://host/path", "registered://host/path?key=value", False],
            ["registered://host/path", "registered://user:pass@host/path", False],
            ["registered://host/path1", "registered://host/path2", False],
        ],
    )
    def test_url_equality(self, url_1, url_2, should_be_equal, register_handler):
        assert (urls.URL(url_1) == urls.URL(url_2)) == should_be_equal

    @pytest.mark.parametrize(
        "original,components,expected",
        [
            (
                "registered://user:password@hostname.com:7744/path?my_arg=my_value",
                {"username": "myuser"},
                "registered://myuser:password@hostname.com:7744/path?my_arg=my_value",
            ),
            (
                "registered://user:password@hostname.com:7744/path?my_arg=my_value",
                {"password": "mypassword"},
                "registered://user:mypassword@hostname.com:7744/path?my_arg=my_value",
            ),
            (
                "registered://user:password@hostname.com:7744/path?my_arg=my_value",
                {"hostname": "google.com"},
                "registered://user:password@google.com:7744/path?my_arg=my_value",
            ),
            (
                "registered://user:password@hostname.com:7744/path?my_arg=my_value",
                {"port": 80},
                "registered://user:password@hostname.com:80/path?my_arg=my_value",
            ),
            (
                "registered://user:password@hostname.com:7744/path?my_arg=my_value",
                {"path": "otherpath"},
                "registered://user:password@hostname.com:7744/otherpath?my_arg=my_value",
            ),
            (
                "registered://user:password@hostname.com:7744/path?my_arg=my_value",
                {"query": {"my_arg": "other_value"}},
                "registered://user:password@hostname.com:7744/path?my_arg=other_value",
            ),
        ],
    )
    def test_copy(self, original, components, expected, register_handler):
        result = urls.URL(original).copy(**components)
        assert result == urls.URL(expected)

    def test_string_hides_password(self, register_handler):
        original = urls.URL("registered://user:password@hostname.com")
        str_url = str(original)
        assert str_url == "registered://user:__secret__word@hostname.com"
