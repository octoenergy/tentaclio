import pytest

from dataio.clients import postgres_client


class TestPostgresClient:
    @pytest.mark.parametrize("url", ["file:///test.file", "ftp://:@localhost", "s3://:@s3"])
    def test_invalid_scheme(self, url):
        with pytest.raises(ValueError):
            postgres_client.PostgresClient(url)

    @pytest.mark.parametrize(
        "url, username, password, hostname, port, database",
        [
            ("postgresql://:@localhost", "", "", "localhost", None, ""),
            ("postgresql://login:pass@localhost", "login", "pass", "localhost", None, ""),
            ("postgresql://:@localhost:5432", "", "", "localhost", 5432, ""),
            ("postgresql://:@localhost:5432/database", "", "", "localhost", 5432, "database"),
        ],
    )
    def test_parsing_postgres_url(self, url, username, password, hostname, port, database):
        client = postgres_client.PostgresClient(url)

        assert client.drivername == "postgresql"
        assert client.host == hostname
        assert client.username == username
        assert client.password == password
        assert client.port == port
        assert client.database == database
