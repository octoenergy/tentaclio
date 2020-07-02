import pytest

from tentaclio.clients import postgres_client


class TestPostgresClient:
    @pytest.mark.parametrize("url", ["file:///test.file", "ftp://:@localhost", "s3://:@s3"])
    def test_invalid_scheme(self, url):
        with pytest.raises(ValueError):
            postgres_client.PostgresClient(url)

    @pytest.mark.parametrize(
        "url, username, password, hostname, port, database, query",
        [
            ("postgresql://:@localhost", "", "", "localhost", None, "", None),
            ("postgresql://login:pass@localhost", "login", "pass", "localhost", None, "", None),
            ("postgresql://:@localhost:5432", "", "", "localhost", 5432, "", None),
            (
                "postgresql://:@localhost:5432/database",
                "",
                "",
                "localhost",
                5432,
                "database",
                None,
            ),
            ("postgresql://localhost?limit=3", None, None, "localhost", None, "", {"limit": "3"}),
        ],
    )
    def test_parsing_postgres_url(self, url, username, password, hostname, port, database, query):
        client = postgres_client.PostgresClient(url)

        assert client.drivername == "postgresql"
        assert client.host == hostname
        assert client.username == username
        assert client.password == password
        assert client.port == port
        assert client.database == database
        assert client.url_query == query

    @pytest.mark.parametrize(
        "env_value, expected", (("test", {"applicationName": "test"}), ("", None),)
    )
    def test_add_application_name_from_env(self, mocker, env_value, expected):
        mocked_os = mocker.patch("tentaclio.clients.postgres_client.os")
        mocked_os.getenv.return_value = env_value
        client = postgres_client.PostgresClient("postgresql://hostname/db")
        assert client.url_query == expected
