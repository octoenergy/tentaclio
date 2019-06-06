import pytest

from tentaclio import URL
from tentaclio.credentials import injection


@pytest.mark.parametrize(
    ["with_creds", "raw", "expected"],
    [
        [
            "scheme://user:password@octo.energy/path",
            "scheme://octo.energy/path",
            "scheme://user:password@octo.energy/path",
        ],
        [
            "scheme://user:password@octo.energy/path/",
            "scheme://octo.energy/path/with/more/elements",
            "scheme://user:password@octo.energy/path/with/more/elements",
        ],
        [
            "scheme://user:password@octo.energy/path/",
            "scheme://octo.energy/path/with/more/elements",
            "scheme://user:password@octo.energy/path/with/more/elements",
        ],
        [
            "scheme://user:password@octo.energy/database",
            "scheme://octo.energy/database::table",
            "scheme://user:password@octo.energy/database::table",
        ],
        [
            "scheme://user:password@octo.energy/database",
            "scheme://hostname/database",  # hostname wildcard
            "scheme://user:password@octo.energy/database",
        ],
        [
            "scheme://user:password@octo.energy:5544/database",
            "scheme://octo.energy/database",
            "scheme://user:password@octo.energy:5544/database",
        ],
        [
            "scheme://user:password@octo.energy/database",
            "scheme://octo.energy/database2",
            "scheme://octo.energy/database2",  # the path is similar but not identical
        ],
        [
            "scheme://user:password@octo.energy:5544/database?key=value",
            "scheme://octo.energy/database",
            "scheme://user:password@octo.energy:5544/database?key=value",
        ],
        [
            "scheme://user:password@octo.energy:5544/database?key=value_1",
            "scheme://octo.energy/database?key=value_2",
            "scheme://user:password@octo.energy:5544/database?key=value_2",
        ],
        [
            "scheme://user:password@octo.energy/",  # trailing slash
            "scheme://octo.energy/file",
            "scheme://user:password@octo.energy/file",
        ],
        [
            "scheme://user:password@octo.energy/",  # trailing slash
            "scheme://octo.energy/path/",
            "scheme://user:password@octo.energy/path/",
        ],
    ],
)
def test_simple_authenticate(with_creds, raw, expected):
    injector = injection.CredentialsInjector()
    with_creds_url = URL(with_creds)
    raw_url = URL(raw)
    expected_url = URL(expected)
    injector.register_credentials(with_creds_url)
    result = injector.inject(raw_url)
    assert expected_url == result


@pytest.mark.parametrize(
    "path_1, path_2, expected",
    [
        ("", "", 0.5),
        ("path", None, 0.5),
        ("path_elem_1", "path_elem_1/path_elem_2", 1),
        ("path_elem_1/path_elem_2", "path_elem_1/path_elem_2", 2),
    ],
)
def test_similarites(path_1, path_2, expected):
    result = injection._similarity(path_1, path_2)
    assert result == expected


def test_hostname_is_wildcard():
    matches = injection._filter_by_hostname(
        URL("scheme://hostname/"), [URL("scheme://google.com/path")]
    )
    assert matches == [URL("scheme://google.com/path")]


def test_filter_by_hostname():
    matches = injection._filter_by_hostname(
        URL("scheme://google.com/"),
        [URL("scheme://google.com/path"), URL("scheme://yahoo.com/path")],
    )
    assert matches == [URL("scheme://google.com/path")]
