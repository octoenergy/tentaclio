import pytest
from click import testing

from tentaclio import cli, urls


@pytest.fixture
def cli_runner():
    return testing.CliRunner()


@pytest.fixture
def mock_compose_url(mocker):
    return mocker.patch.object(urls.URL, "from_components")


class TestComposeURL:
    def test_url_composer(self, cli_runner, mock_compose_url):
        result = cli_runner.invoke(
            cli.compose_url,
            [
                "--scheme",
                "file",
                "--hostname",
                "host",
                "--port",
                "1234",
                "--path",
                "/path/to",
                "--username",
                "user",
                "--password",
                "pw",
            ],
        )

        assert result.exit_code == 0
        mock_compose_url.assert_called_once_with(
            scheme="file",
            username="user",
            password="pw",
            hostname="host",
            port=1234,
            path="/path/to",
            query=None,
        )

    def test_key_value_pairs(self, cli_runner, mock_compose_url):
        result = cli_runner.invoke(
            cli.compose_url,
            [
                "--scheme",
                "file",
                "--path",
                "/path/to",
                "--key",
                "key1",
                "value1",
                "--key",
                "key2",
                "value2",
            ],
        )

        assert result.exit_code == 0
        mock_compose_url.assert_called_once_with(
            scheme="file",
            username=None,
            password=None,
            hostname=None,
            port=None,
            path="/path/to",
            query={"key1": "value1", "key2": "value2"},
        )

    def test_with_invalid_port(self, cli_runner, mock_compose_url):
        result = cli_runner.invoke(
            cli.compose_url, ["--scheme", "http", "--hostname", "host", "--port", "not_an_int"]
        )

        assert result.exit_code != 0
        assert "is not a valid integer" in result.output

    def test_with_no_host_or_path(self, cli_runner, mock_compose_url):
        result = cli_runner.invoke(cli.compose_url, ["--scheme", "http"])

        assert result.exit_code != 0
        assert "Provide at least one of" in result.output

    def test_command(self, cli_runner):
        result = cli_runner.invoke(
            cli.compose_url,
            [
                "--scheme",
                "file",
                "--hostname",
                "host",
                "--port",
                "1234",
                "--path",
                "/path/to",
                "--username",
                "user",
                "--password",
                "pw",
            ],
        )

        assert result.exit_code == 0
        assert result.output == "file://user:pw@host:1234/path/to"
