import io

import pytest

from dataio import urls
from dataio.credentials import injection, reader


@pytest.fixture
def no_creds_yaml():
    return """
example: []
"""


@pytest.fixture
def creds_yaml():
    return """
secrets:
    local_ftp: ftp://user:password@local.com
    remote_db: postgresql://user_db:password_db@db.com/database
"""


@pytest.fixture
def creds_yaml_bad_url():
    return """
secrets:
    local_ftp: ftp://user:password@local.com
    remote_db: I'm @not a V&?!lid url
"""


def test_bad_yaml():
    with pytest.raises(Exception):
        data = io.StringIO("sadfsaf")
        reader.add_credentials_from_reader(injection.CredentialsInjector(), data)


def test_no_credentials_in_file(no_creds_yaml):
    with pytest.raises(KeyError):
        data = io.StringIO(no_creds_yaml)
        reader.add_credentials_from_reader(injection.CredentialsInjector(), data)


@pytest.mark.parametrize(
    "url, expected",
    [
        ("ftp://local.com/file.txt", "ftp://user:password@local.com/file.txt"),
        ("postgresql://db.com/database", "postgresql://user_db:password_db@db.com/database"),
    ],
)
def test_credentials(url, expected, creds_yaml):
    data = io.StringIO(creds_yaml)
    injector = reader.add_credentials_from_reader(injection.CredentialsInjector(), data)

    result = injector.inject(urls.URL(url))
    assert result == urls.URL(expected)


def test_credentials_bad_url(register_handler, creds_yaml_bad_url):
    data = io.StringIO(creds_yaml_bad_url)
    with pytest.raises(Exception):
        reader.add_credentials_from_reader(injection.CredentialsInjector(), data)
