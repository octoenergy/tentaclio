import io

import pytest

from tentaclio import urls
from tentaclio.credentials import TentaclioFileError, injection, reader


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


@pytest.fixture
def creds_yaml_env_variables():
    return """
secrets:
    local_ftp: ftp://${USER}:${PASSWORD}@local.com
    remote_db: postgresql://${USER_DB}:${PASSWORD_DB}@db.com/database
"""


@pytest.fixture
def creds_yaml_bad_env_variables():
    return """
secrets:
    local_ftp: ftp://${FAKE_USER}:${FAKE_PASSWORD}@local.com
    remote_db: postgresql://${FAKE_USER_DB}:${FAKE_PASSWORD_DB}@db.com/database
"""


def test_bad_yaml():
    with pytest.raises(TentaclioFileError):
        data = io.StringIO("sadfsaf")
        reader.add_credentials_from_reader(injection.CredentialsInjector(), data)


def test_no_credentials_in_file(no_creds_yaml):
    with pytest.raises(TentaclioFileError):
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


def test_credentials_bad_url(creds_yaml_bad_url):
    data = io.StringIO(creds_yaml_bad_url)
    with pytest.raises(Exception):
        reader.add_credentials_from_reader(injection.CredentialsInjector(), data)


@pytest.mark.parametrize(
    "url, expected",
    [
        ("ftp://local.com/file.txt", "ftp://user:password@local.com/file.txt"),
        ("postgresql://db.com/database", "postgresql://user_db:password_db@db.com/database"),
    ],
)
def test_credentials_env_variable(url, expected, creds_yaml_env_variables, monkeypatch):
    monkeypatch.setenv("USER", "user")
    monkeypatch.setenv("USER_DB", "user_db")
    monkeypatch.setenv("PASSWORD", "password")
    monkeypatch.setenv("PASSWORD_DB", "password_db")

    data = io.StringIO(creds_yaml_env_variables)
    injector = reader.add_credentials_from_reader(injection.CredentialsInjector(), data)

    result = injector.inject(urls.URL(url))
    assert result == urls.URL(expected)


def test_credentials_bad_env_variable(creds_yaml_bad_env_variables):
    data = io.StringIO(creds_yaml_bad_env_variables)
    with pytest.raises(EnvironmentError):
        reader.add_credentials_from_reader(injection.CredentialsInjector(), data)
