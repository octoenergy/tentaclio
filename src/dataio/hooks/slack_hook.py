import io
import json

from dataio.clients import http_client

__all__ = ["SlackHook"]


class SlackHook:
    """
    Slack incoming web hook (POST messages)
    """

    client: http_client.HTTPClient

    def __init__(self, url: str) -> None:
        self.client = http_client.HTTPClient(url)

    def notify(
        self, message: str, channel: str = None, username: str = None, icon_emoji: str = None
    ) -> None:
        body = self._build_request_body(
            message, channel=channel, username=username, icon_emoji=icon_emoji
        )
        stream = io.BytesIO(bytes(body, encoding="utf-8"))
        # Post streamed request
        self.client.put(stream)

    # Helpers:

    @staticmethod
    def _build_request_body(
        message: str, channel: str = None, username: str = None, icon_emoji: str = None
    ) -> str:
        # Fetch message payload
        payload = dict(text=message)
        if channel:
            payload["channel"] = channel
        if username:
            payload["username"] = username
        if icon_emoji:
            payload["icon_emoji"] = icon_emoji
        return json.dumps(payload)
