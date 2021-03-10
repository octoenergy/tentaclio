import pytest
from tentaclio.clients.databricks_client import build_odbc_connection_string, get_param_from_url_query_or_env_var


def test_build_odbc_connection_string():
    output = build_odbc_connection_string(DRIVER="simba",
                                          PORT=443,
                                          UID="token")
    expected = "DRIVER%3Dsimba%3BPORT%3D443%3BUID%3Dtoken"
    assert output == expected


@pytest.mark.parametrize("env_value, expected", [
    ("env_value", "env_value"),
    ("", "url_value"),
])
def test_get_param_from_url_query_or_env_var(monkeypatch, env_value, expected):
    monkeypatch.setenv("TENTACLIO__KEY", env_value)
    url_query = {"url_key": "url_value"}
    output = get_param_from_url_query_or_env_var("TENTACLIO__KEY", "url_key", url_query)
    assert output == expected


def test_get_param_from_url_query_or_env_var_when_no_env_var():
    url_query = {"url_key": "url_value"}
    output = get_param_from_url_query_or_env_var("TENTACLIO__KEY", "url_key", url_query)
    assert output == "url_value"


def test_get_param_from_url_query_or_env_var_when_no_env_var_or_url_value(caplog):
    output = get_param_from_url_query_or_env_var("TENTACLIO__KEY", "url_key", dict())
    assert output == ""
    assert "TENTACLIO__KEY not found in env vars and url_key not found in url query" in caplog.text
