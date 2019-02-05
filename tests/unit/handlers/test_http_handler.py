import json

import pytest
import requests

from dataio import api, urls


@pytest.mark.parametrize(
    "url,scheme", [("http://host.com/request", "http"), ("https://host.com/request", "https")]
)
def test_handler_is_registered(url, scheme):
    assert urls.URL(url).scheme == scheme


def test_open_http_url_reading(mocker):
    payload = bytes(json.dumps([{"result": "everything is alright"}]), "utf-8")
    mocked_response = mocker.Mock(content=payload)
    mocked_session_call = mocker.patch.object(
        requests.Session, "send", return_value=mocked_response
    )

    with api.open_reader("http://host.com/request") as reader:
        content = reader.read()

    assert content == payload
    mocked_session_call.assert_called_once()


def test_open_http_url_writer():
    content = json.dumps([{"result": "nothing is alright"}])

    with pytest.raises(NotImplementedError):
        with api.open_writer("http://host.com/request") as writer:
            writer.write(content)
