import io
import json

from dataio.clients import http_client


__all__ = ["SlackHook"]


class SlackHook(http_client.HTTPClient):
    """
    Slack incoming web hook (POST messages)
    """

    def __init__(self, url: str) -> None:
        super().__init__(url)

    def notify(
        self, message: str, channel: str = None, username: str = None, icon_emoji: str = None
    ) -> None:
        body = self._build_request_body(
            message, channel=channel, username=username, icon_emoji=icon_emoji
        )
        # Body streamed as StringIO reader
        f = io.StringIO()
        f.write(body)
        f.seek(0)
        # Post streamed request
        super().put(f)

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
