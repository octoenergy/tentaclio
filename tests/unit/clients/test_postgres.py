import pytest
from dataio.clients import exceptions, postgres_client


class TestPostgresClient:
    @pytest.mark.parametrize("url", ["file:///test.file", "ftp://:@localhost", "s3://:@s3"])
    def test_invalid_scheme(self, url):
        with pytest.raises(exceptions.PostgresError):
            postgres_client.PostgresClient(url)

    @pytest.mark.parametrize(
        "url,username,password,hostname,port,path",
        [
            ("postgresql://:@localhost", "", "", "localhost", None, ""),
            ("postgresql://login:pass@localhost", "login", "pass", "localhost", None, ""),
            ("postgresql://:@localhost:5432", "", "", "localhost", 5432, ""),
            ("postgresql://:@localhost:5432/database", "", "", "localhost", 5432, "database"),
        ],
    )
    def test_parsing_postgres_url(self, url, username, password, hostname, port, path):
        parsed_url = postgres_client.PostgresClient(url).url

        assert parsed_url.scheme == "postgresql"
        assert parsed_url.hostname == hostname
        assert parsed_url.username == username
        assert parsed_url.password == password
        assert parsed_url.port == port
        assert parsed_url.path == path
