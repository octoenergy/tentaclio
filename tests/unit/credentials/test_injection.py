import pytest

from dataio import URL
from dataio.credentials import injection


@pytest.mark.parametrize(
    ["with_creds", "raw", "expected"],
    [
        [
            "registered://user:password@octo.energy/path",
            "registered://octo.energy/path",
            "registered://user:password@octo.energy/path",
        ],
        [
            "registered://user:password@octo.energy/path/",
            "registered://octo.energy/path/with/more/elements",
            "registered://user:password@octo.energy/path/with/more/elements",
        ],
        [
            "registered://user:password@octo.energy/path/",
            "registered://octo.energy/path/with/more/elements",
            "registered://user:password@octo.energy/path/with/more/elements",
        ],
        [
            "registered://user:password@octo.energy/database",
            "registered://octo.energy/database::table",
            "registered://user:password@octo.energy/database::table",
        ],
        [
            "registered://user:password@octo.energy/database",
            "registered://hostname/database",  # hostname wildcard
            "registered://user:password@octo.energy/database",
        ],
        [
            "registered://user:password@octo.energy:5544/database",
            "registered://octo.energy/database",
            "registered://user:password@octo.energy:5544/database",
        ],
        [
            "registered://user:password@octo.energy/database",
            "registered://octo.energy/database2",
            "registered://octo.energy/database2",  # the path is similar but not identical
        ],
        [
            "registered://user:password@octo.energy:5544/database?key=value",
            "registered://octo.energy/database",
            "registered://user:password@octo.energy:5544/database?key=value",
        ],
        [
            "registered://user:password@octo.energy:5544/database?key=value_1",
            "registered://octo.energy/database?key=value_2",
            "registered://user:password@octo.energy:5544/database?key=value_2",
        ],
    ],
)
def test_simple_authenticate(with_creds, raw, expected, register_handler):
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


def test_hostname_is_wildcard(register_handler):
    matches = injection._filter_by_hostname(
        URL("registered://hostname/"), [URL("registered://google.com/path")]
    )
    assert matches == [URL("registered://google.com/path")]


def test_filter_by_hostname(register_handler):
    matches = injection._filter_by_hostname(
        URL("registered://google.com/"),
        [URL("registered://google.com/path"), URL("registered://yahoo.com/path")],
    )
    assert matches == [URL("registered://google.com/path")]
