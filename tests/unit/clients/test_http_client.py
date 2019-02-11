import pytest

from dataio.clients import exceptions, http_client


@pytest.fixture()
def mocked_http_conn(mocker):
    with mocker.patch.object(http_client.HTTPClient, "connect", return_value=mocker.Mock()):
        yield


class TestHTTPClient:
    @pytest.mark.parametrize("url", ["file:///test.file", "sftp://:@localhost", "s3://:@s3"])
    def test_invalid_scheme(self, url):
        with pytest.raises(ValueError):
            http_client.HTTPClient(url)

    @pytest.mark.parametrize(
        "url,username,password,hostname,port,path",
        [
            # Parse credentials
            ("http://:@host.com", None, None, "host.com", None, ""),
            ("http://login:pass@host.com", "login", "pass", "host.com", None, ""),
            # Parse web components
            ("http://host.com", None, None, "host.com", None, ""),
            ("http://host.com:8080", None, None, "host.com", 8080, ""),
            ("http://host.com:8080/endpoint", None, None, "host.com", 8080, "/endpoint"),
        ],
    )
    def test_parsing_http_url(self, url, username, password, hostname, port, path):
        client = http_client.HTTPClient(url)

        assert client.protocol == "http"
        assert client.hostname == hostname
        assert client.username == username
        assert client.password == password
        assert client.port == port
        assert client.endpoint == path

    @pytest.mark.parametrize(
        "url,path",
        [
            # Missing endpoint
            ("https://host.com", None),
            # Missing hostname
            ("https://:8080", "/endpoint"),
            ("https://:@:8080", "/endpoint"),
        ],
    )
    def test_get_invalid_endpoint(self, url, path, mocker, mocked_http_conn):
        mocked_writer = mocker.Mock()

        with pytest.raises(exceptions.HTTPError):

            with http_client.HTTPClient(url) as client:
                client.get(mocked_writer, endpoint=path)

    @pytest.mark.parametrize(
        "base_url,endpoint,auth,full_url",
        [
            # Test auth
            ("http://user:key@host.com/request", "", ("user", "key"), "http://host.com/request"),
            ("http://:@host.com/request", "", None, "http://host.com/request"),
            ("http://host.com/request", "", None, "http://host.com/request"),
            # Test escaped endpoint
            ("http://host.com", "/request", None, "http://host.com/request"),
            ("http://host.com", "request", None, "http://host.com/request"),
            # test overlapped endpoint
            ("http://host.com/v1/api", "/v1/api/request", None, "http://host.com/v1/api/request"),
        ],
    )
    def test_fetching_url_endpoint(self, base_url, endpoint, auth, full_url):
        with http_client.HTTPClient(base_url) as client:

            assert client.conn.auth == auth
            assert client._fetch_url(endpoint) == full_url
