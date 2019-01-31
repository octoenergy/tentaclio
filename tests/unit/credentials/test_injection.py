from dataio import URL
from dataio.credentials import injection

import pytest


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
            "registered://user:password@octo.energy/database",
            "registered://octo.energy/database2",
            "registered://octo.energy/database2",  # the path is similar but not identical
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
