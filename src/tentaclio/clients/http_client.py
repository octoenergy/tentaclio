"""HTTP Stream client."""
import io
from typing import Optional
from urllib import parse

import requests

from tentaclio import protocols

from . import base_client, decorators, exceptions


__all__ = ["HTTPClient"]


DEFAULT_TIMEOUT = 10.0

DEFAULT_HEADERS = {"Accept": "application/json"}


class HTTPClient(base_client.StreamClient):
    """HTTP stream client.

    This client is useful when dealing inputs that may change from local files to http,
    or from s3 to http in simple usecases. We're not intending to wrap requests or rewrite it.
    """

    allowed_schemes = ["http", "https"]

    conn: requests.Session
    timeout: float
    headers: dict
    protocol: str
    username: Optional[str]
    password: Optional[str]
    hostname: str
    port: Optional[int]
    endpoint: str

    def __init__(
        self, url: str, default_timeout: float = None, default_headers: dict = None
    ) -> None:
        """Create a new http/https client based on the passed url and extra params."""
        # Default connection timeout at 10''
        self.timeout = default_timeout or DEFAULT_TIMEOUT
        # Default JSON response back
        self.headers = default_headers or DEFAULT_HEADERS
        super().__init__(url)

        self.protocol = self.url.scheme

        if self.url.hostname is None:
            raise exceptions.HTTPError(f"Missing URL hostname")
        self.hostname = self.url.hostname

        self.port = self.url.port
        self.endpoint = self.url.path

        # Enforce no empty credentials
        self.username = None if self.url.username == "" else self.url.username
        self.password = None if self.url.password == "" else self.url.password

    # Connection methods:

    def _connect(self) -> requests.Session:
        session = requests.Session()

        # credentials provided
        if self.username and self.password:
            session.auth = (self.username, self.password)

        # Non-empty header
        if self.headers:
            session.headers.update(self.headers)

        return session

    # Stream methods:

    @decorators.check_conn
    def get(
        self,
        writer: protocols.ByteWriter,
        endpoint: str = None,
        params: dict = None,
        options: dict = None,
    ) -> None:
        """Read the contents from the url and write them into the provided writer.

        Arguments:
            :end_point: Path to append to the url passed in the constructor.
            :params: Url params to add
            :options: More options for the request library.
        """
        url = self._fetch_url(endpoint or "")

        request = self._build_request("GET", url, default_params=params)
        response = self._send_request(request, default_options=options)

        writer.write(response.content)

    @decorators.check_conn
    def put(
        self,
        reader: protocols.ByteReader,
        endpoint: str = None,
        params: dict = None,
        options: dict = None,
    ) -> None:
        """Write the contents of the provided reader into the url using POST.

        Arguments:
            :end_point: Path to append to the url passed in the constructor.
            :params: Url params to add
            :options: More options for the request library.
        """
        url = self._fetch_url(endpoint or "")
        buff = io.StringIO(bytes(reader.read()).decode(encoding="utf-8"))
        request = self._build_request("POST", url, default_data=buff, default_params=params)
        self._send_request(request, default_options=options)

    # Helpers:

    def _fetch_url(self, endpoint: str) -> str:
        if endpoint == "" and self.endpoint == "":
            raise exceptions.HTTPError("Missing URL end point")
        # Fetch full base URL
        base_url = parse.urlunparse((self.protocol, self.hostname, self.endpoint, "", "", ""))
        return parse.urljoin(base_url, endpoint)

    def _build_request(
        self,
        method: str,
        url: str,
        default_data: protocols.Reader = None,
        default_params: dict = None,
    ):
        data = default_data or []
        params = default_params or {}

        if method == "GET":
            # GET uses params
            request = requests.Request(method, url, params=params, headers=self.headers)
        elif method == "POST":
            # POST uses data & params
            request = requests.Request(method, url, data=data, params=params, headers=self.headers)
        else:
            raise NotImplementedError

        return self.conn.prepare_request(request)

    def _send_request(self, request: requests.PreparedRequest, default_options: dict = None):
        options = default_options or {}

        response = self.conn.send(
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
