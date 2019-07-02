import json

import pytest
import requests

from tentaclio import api, urls


TEST_PAYLOAD = json.dumps([{"result": "nothing is alright"}])


@pytest.mark.parametrize(
    "url,scheme", [("http://host.com/request", "http"), ("https://host.com/request", "https")]
)
def test_handler_is_registered(url, scheme):
    assert urls.URL(url).scheme == scheme


@pytest.mark.parametrize(
    "mode,content, expected_content",
    [
        ("b", bytes(TEST_PAYLOAD, "utf-8"), bytes(TEST_PAYLOAD, "utf-8")),
        ("t", bytes(TEST_PAYLOAD, "utf-8"), TEST_PAYLOAD),
    ],
)
def test_open_http_url_reading(mode, content, expected_content, mocker):
    mocked_response = mocker.Mock(content=content)
    mocked_session_call = mocker.patch.object(
        requests.Session, "send", return_value=mocked_response
    )

    with api.open("http://host.com/request", mode=mode) as reader:
        read_content = reader.read()

    assert read_content == expected_content
    mocked_session_call.assert_called_once()


@pytest.mark.skip("This is actually implmented. Write test")
@pytest.mark.parametrize(
    "mode,content", [("wb", bytes(TEST_PAYLOAD, "utf-8")), ("wt", TEST_PAYLOAD)]
)
def test_open_http_url_writer(mode, content):
    with pytest.raises(NotImplementedError):
        with api.open("http://host.com/request", mode=mode) as writer:
            writer.write(content)
