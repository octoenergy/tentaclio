import io
import json

from . import decorators, http_client


__all__ = ["SlackClient"]


class SlackClient(http_client.HTTPClient):
    """
    Slack incoming web hook (POST messages)
    """

    def __init__(self, url: str) -> None:
        super().__init__(url)

    @decorators.check_conn()
    def notify(
        self, message: str, channel: str = None, username: str = None, icon_emoji: str = None
    ) -> None:
        body = self._build_request_body(
            message, channel=channel, username=username, icon_emoji=icon_emoji
        )
        # Body streamed as string
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
