import json

import pytest
import requests
from dataio import api, urls

TEST_PAYLOAD = json.dumps([{"result": "nothing is alright"}])


@pytest.mark.parametrize(
    "url,scheme", [("http://host.com/request", "http"), ("https://host.com/request", "https")]
)
def test_handler_is_registered(url, scheme):
    assert urls.URL(url).scheme == scheme


@pytest.mark.parametrize(
    "mode,content", [("b", bytes(TEST_PAYLOAD, "utf-8")), ("s", TEST_PAYLOAD)]
)
def test_open_http_url_reading(mode, content, mocker):
    mocked_response = mocker.Mock(content=content)
    mocked_session_call = mocker.patch.object(
        requests.Session, "send", return_value=mocked_response
    )

    with api.open_reader("http://host.com/request", mode) as reader:
        content = reader.read()

    assert content == content
    mocked_session_call.assert_called_once()


@pytest.mark.parametrize(
    "mode,content", [("b", bytes(TEST_PAYLOAD, "utf-8")), ("s", TEST_PAYLOAD)]
)
def test_open_http_url_writer(mode, content):
    with pytest.raises(NotImplementedError):
        with api.open_writer("http://host.com/request", mode) as writer:
            writer.write(content)
