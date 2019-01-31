import pytest
from click.testing import CliRunner

from dataio.cli.url import command_compose_url


@pytest.fixture
def mock_compose_url(mocker):
    return mocker.patch('dataio.cli.url._compose_url')


class TestComposeURL:
    def test_url_composer(self, mock_compose_url):
        result = CliRunner().invoke(
            command_compose_url, [
                '--scheme', 'file', '--hostname', 'host', '--port', '1234', '--path', '/path/to',
                '--username', 'user', '--password', 'pw',
            ]
        )

        assert result.exit_code == 0
        mock_compose_url.assert_called_once_with(
            scheme='file',
            username='user',
            password='pw',
            hostname='host',
            port=1234,
            path='/path/to',
            query=None
        )

    def test_key_value_pairs(self, mock_compose_url):
        result = CliRunner().invoke(
            command_compose_url, [
                '--scheme', 'file', '--path', '/path/to',
                '--key', 'key1', 'value1', '--key', 'key2', 'value2'
            ]
        )

        assert result.exit_code == 0
        mock_compose_url.assert_called_once_with(
            scheme='file',
            username=None,
            password=None,
            hostname=None,
            port=None,
            path='/path/to',
            query={'key1': 'value1', 'key2': 'value2'}
        )

    def test_with_invalid_port(self, mock_compose_url):
        result = CliRunner().invoke(
            command_compose_url,
            ['--scheme', 'http', '--hostname', 'host', '--port', 'not_an_int']
        )

        assert result.exit_code != 0
        assert b'is not a valid integer' in result.stdout_bytes

    def test_with_no_host_or_path(self, mock_compose_url):
        result = CliRunner().invoke(
            command_compose_url,
            ['--scheme', 'http']
        )

        assert result.exit_code != 0
        assert b'Provide at least one of' in result.stdout_bytes

    def test_command(self):
        result = CliRunner().invoke(
            command_compose_url, [
                '--scheme', 'file', '--hostname', 'host', '--port', '1234', '--path', '/path/to',
                '--username', 'user', '--password', 'pw',
            ]
        )

        assert result.exit_code == 0
        assert result.stdout_bytes == b'file://user:pw@host:1234/path/to'
