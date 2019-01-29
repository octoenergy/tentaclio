from dataio import Reader, Writer
from dataio.urls import URL, URLError, URLHandlerRegistry

import pytest


class FakeHandler(object):
    def open_reader_for(self, url: "URL", extras: dict) -> Reader:
        pass

    def open_writer_for(self, url: "URL", extras: dict) -> Writer:
        pass


@pytest.fixture
def register_handler(fake_handler):
    URL.register_handler("registered", fake_handler)
    return fake_handler


@pytest.fixture
def fake_handler():
    return FakeHandler()


class TestRegistry(object):
    def test_register_handler(self, fake_handler):
        registry = URLHandlerRegistry()
        registry.register("scheme", fake_handler)
        result = registry.get_handler("scheme")
        assert fake_handler is result

    def test_unknown_handler(self):
        registry = URLHandlerRegistry()
        with pytest.raises(URLError):
            registry.get_handler("scheme")

    def test_contains_handler(self, fake_handler):
        registry = URLHandlerRegistry()
        registry.register("http", fake_handler)
        assert "http" in registry
        assert "https" not in registry


class TestURL:

    # Generic parsing rules:
    def test_missing_url(self):
        with pytest.raises(URLError):
            URL(None)

    def test_unknown_url_scheme(self):
        url = "notregistered://localhost"
        with pytest.raises(URLError):
            URL(url)

    def test_known_url_scheme(self, register_handler):
        url = "registered://localhost"
        # well, this shouldn't blow
        URL(url)

    @pytest.mark.parametrize(
        "url,username,password",
        [
            ("registered://:@localhost", "", ""),
            ("registered://abc_def%40123.com:@localhost", "abc_def@123.com", ""),
            (
                "registered://:%40%60%60Z_%24-%24%405%25Ky%2F@localhost",
                "",
                "@``Z_$-$@5%Ky/",
            ),
        ],
    )
    def test_url_escaped_fields(self, url, username, password, register_handler):
        parsed_url = URL(url)

        assert parsed_url.username == username
        assert parsed_url.password == password

    # File URL:
    # TODO move to base clients

    @pytest.mark.skip()
    @pytest.mark.parametrize(
        "url,path",
        [
            ("file:///test.file", "/test.file"),
            ("file:///dir/test.file", "/dir/test.file"),
        ],
    )
    def test_parsing_file_url(self, url, path):
        parsed_url = URL(url)

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
        self,
        url,
        scheme,
        username,
        password,
        hostname,
        port,
        path,
        query,
        register_handler,
    ):
        parsed_url = URL.from_components(
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
