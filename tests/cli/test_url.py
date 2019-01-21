from click.testing import CliRunner

from dataio.cli.url import command_compose_url


def test_url_composer(mocker):
    mock_compose_url = mocker.patch("dataio.cli.url._compose_url")

    result = CliRunner().invoke(command_compose_url, ["--scheme", "file", "--path", "/path/to"])

    assert result.exit_code == 0
    mock_compose_url.assert_called_once_with(
        scheme="file",
        username=None,
        password=None,
        hostname=None,
        port=None,
        path="/path/to",
        query=None,
    )
