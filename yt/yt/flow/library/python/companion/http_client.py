"""Shared HTTP(S) clients handed to user code through the runtime context.

The Python mirror of the C++ companion's ``IRuntimeInitContext::GetHttpClient()`` /
``GetHttpsClient()``: the companion builds both clients once per process from the
``http_client_config`` and ``https_client_config`` blocks of the worker-provided
companion config, and user code reaches them via ``ctx.http_client`` and
``ctx.https_client``.

The clients share one pooled session per scheme and are safe for concurrent use
from every serving thread. Per-request headers, body and timeout are passed at
the call site; the shared instance itself must not be mutated.
"""

import http.cookiejar
import threading
from typing import Any, Dict, Optional, Tuple, Union

try:
    import yt.packages.requests as requests
    from yt.packages.requests.adapters import HTTPAdapter
except ImportError:
    import requests
    from requests.adapters import HTTPAdapter

# Connect and read timeout, seconds; a per-request |timeout| argument overrides it.
DEFAULT_REQUEST_TIMEOUT = (10.0, 60.0)
# Connection pool size when the config leaves it unset or zero.
DEFAULT_MAX_IDLE_CONNECTIONS = 8
# Following redirects is opt-in for parity with the C++ client (max_redirect_count=0).
DEFAULT_MAX_REDIRECT_COUNT = 0


def _map_get(mapping: Dict[Any, Any], key: str, default: Any = None) -> Any:
    """Fetch by key from a parsed YSON map whose keys may be str or bytes."""
    if not isinstance(mapping, dict):
        return default
    if key in mapping:
        return mapping[key]
    return mapping.get(key.encode("utf-8"), default)


class _NoCookiePolicy(http.cookiejar.DefaultCookiePolicy):
    """Cookie policy that blocks storing and sending cookies.

    The client is shared by every serving thread and every computation of the
    process, so a cookie received while serving one batch must never leak into
    another batch's requests.
    """

    def set_ok(self, cookie, request):
        return False

    def return_ok(self, cookie, request):
        return False


class HttpResponse:
    """Response of an HTTP request made through an #HttpClient."""

    def __init__(self, response):
        self._response = response

    @property
    def status_code(self) -> int:
        """HTTP status code."""
        return self._response.status_code

    @property
    def ok(self) -> bool:
        """Whether the status code is not an error (4xx or 5xx)."""
        return self._response.ok

    @property
    def url(self) -> str:
        """Final URL of the response after redirects."""
        return self._response.url

    @property
    def headers(self) -> Dict[str, str]:
        """Response headers as returned by the client."""
        return dict(self._response.headers)

    @property
    def content(self) -> bytes:
        """Response body as bytes."""
        return self._response.content

    @property
    def text(self) -> str:
        """Response body decoded with the encoding the response declares."""
        return self._response.text

    def json(self, **kwargs) -> Any:
        """Response body decoded as JSON."""
        return self._response.json(**kwargs)


class HttpClient:
    """Thread-safe pooled HTTP client for user code.

    Wraps one pooled session used by every serving thread of the companion.
    The client is stateless: no cookies are stored or sent. Redirects are not
    followed by default, the 3xx response itself is returned. Request failures
    surface as client exceptions; error status codes do not raise, check
    #HttpResponse.status_code instead.
    """

    def __init__(self, config: Optional[Dict[Any, Any]] = None):
        max_idle_connections = int(_map_get(config, "max_idle_connections") or 0)
        pool_size = max_idle_connections or DEFAULT_MAX_IDLE_CONNECTIONS
        max_redirect_count = int(_map_get(config, "max_redirect_count", DEFAULT_MAX_REDIRECT_COUNT))

        self._session = requests.Session()
        self._session.cookies.set_policy(_NoCookiePolicy())
        self._default_timeout: Union[float, Tuple[float, float]] = DEFAULT_REQUEST_TIMEOUT
        self._max_redirect_count = max(0, max_redirect_count)
        # One adapter serves both schemes, so both share the pool bounds.
        adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)
        # With redirects disabled the limit is never consulted, and setting it
        # to zero would break even the no-follow path (the client still computes
        # Response.next): keep the requests default instead.
        if self._max_redirect_count > 0:
            self._session.max_redirects = self._max_redirect_count

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        data: Any = None,
        json: Any = None,
        timeout: Optional[Union[float, Tuple[float, float]]] = None,
    ) -> HttpResponse:
        """Send one request; see :meth:`requests.Session.request` for the arguments."""
        response = self._session.request(
            method,
            url,
            headers=headers,
            params=params,
            data=data,
            json=json,
            allow_redirects=self._max_redirect_count > 0,
            timeout=timeout if timeout is not None else self._default_timeout,
        )
        return HttpResponse(response)

    def get(self, url: str, **kwargs) -> HttpResponse:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> HttpResponse:
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs) -> HttpResponse:
        return self.request("PUT", url, **kwargs)

    def patch(self, url: str, **kwargs) -> HttpResponse:
        return self.request("PATCH", url, **kwargs)

    def delete(self, url: str, **kwargs) -> HttpResponse:
        return self.request("DELETE", url, **kwargs)

    def head(self, url: str, **kwargs) -> HttpResponse:
        return self.request("HEAD", url, **kwargs)


class HttpClients:
    """The plain-HTTP and HTTPS clients of this companion process."""

    def __init__(self, http: HttpClient, https: HttpClient):
        self.http = http
        self.https = https


def create_http_clients(companion_config: Optional[Dict[Any, Any]]) -> HttpClients:
    """Build the clients from the parsed companion config; missing blocks fall back to defaults."""
    config = companion_config or {}
    return HttpClients(
        http=HttpClient(_map_get(config, "http_client_config")),
        https=HttpClient(_map_get(config, "https_client_config")),
    )


_clients: Optional[HttpClients] = None
_clients_lock = threading.Lock()


def get_http_clients() -> HttpClients:
    """Process-wide clients, built once from the companion config.

    Outside a companion process (tests, embedded use) the config is empty and
    the clients are built with defaults.
    """
    global _clients
    with _clients_lock:
        if _clients is None:
            from .server import _load_companion_config_from_env

            _clients = create_http_clients(_load_companion_config_from_env())
        return _clients
