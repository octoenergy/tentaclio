import io
from typing import Optional
from urllib import parse

import requests

from . import base_client, decorators, exceptions


__all__ = ["HTTPClient"]


DEFAULT_TIMEOUT = 10.0

DEFAULT_HEADERS = {"Content-type": "application/json", "Accept": "application/json"}


class HTTPClient(base_client.StreamClient):
    """
    Generic HTTP hook
    """

    conn: Optional[requests.Session]
    timeout: float
    headers: dict

    def __init__(
        self, url: str, default_timeout: float = None, default_headers: dict = None
    ) -> None:
        # Default connection timeout at 10''
        self.timeout = default_timeout or DEFAULT_TIMEOUT
        # Default JSON response back
        self.headers = default_headers or DEFAULT_HEADERS
        super().__init__(url)

        if self.url.scheme not in ("http", "https"):
            raise exceptions.HTTPError(f"Incorrect scheme {self.url.scheme}")

        # Enforce no empty credentials
        if self.url.username == "":
            self.url.username = None
        if self.url.password == "":
            self.url.password = None

    # Connection methods:

    def connect(self) -> requests.Session:
        session = requests.Session()

        # credentials provided
        if self.url.username and self.url.password:
            session.auth = (self.url.username, self.url.password)

        # Non-empty header
        if self.headers:
            session.headers.update(self.headers)

        return session

    # Stream methods:

    @decorators.check_conn()
    def get(self, endpoint: str = None, params: dict = None, options: dict = None) -> io.StringIO:
        url = self._fetch_url(endpoint or "")

        request = self._build_request("GET", url, default_params=params)
        response = self._send_request(request, default_options=options)

        return response.content

    @decorators.check_conn()
    def put(
        self, data: io.StringIO, endpoint: str = None, params: dict = None, options: dict = None
    ) -> None:
        url = self._fetch_url(endpoint or "")

        request = self._build_request("POST", url, default_data=data, default_params=params)
        self._send_request(request, default_options=options)

    # Helpers:

    def _fetch_url(self, endpoint: str) -> str:
        if endpoint == "" and self.url.path == "":
            raise exceptions.HTTPError("Missing URL end point")
        # Enforce no empty hostname
        if self.url.hostname is None:
            raise exceptions.HTTPError(f"Missing URL hostname")
        # Fetch full base URL
        base_url = parse.urlunparse(
            (self.url.scheme, self.url.hostname, self.url.path, "", "", "")
        )
        return parse.urljoin(base_url, endpoint)

    def _build_request(
        self, method: str, url: str, default_data: io.StringIO = None, default_params: dict = None
    ):
        data = default_data or {}
        params = default_params or {}

        if method == "GET":
            # GET uses params
            request = requests.Request(method, url, params=params, headers=self.headers)
        elif method == "POST":
            # POST uses data & params
            request = requests.Request(method, url, data=data, params=params, headers=self.headers)
        else:
            raise NotImplementedError

        return self.conn.prepare_request(request)  # type: ignore

    def _send_request(self, request: requests.PreparedRequest, default_options: dict = None):
        options = default_options or {}

        response = self.conn.send(  # type: ignore
            request,
            stream=options.get("stream", False),
            verify=options.get("verify", False),
            proxies=options.get("proxies", {}),
            cert=options.get("cert"),
            timeout=options.get("timeout", self.timeout),
            allow_redirects=options.get("allow_redirects", True),
        )

        if options.get("check_response", True):
            self._check_response(response)

        return response

    @staticmethod
    def _check_response(response: requests.Response) -> None:
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise exceptions.HTTPError(f"{response.status_code}: {response.reason}")
