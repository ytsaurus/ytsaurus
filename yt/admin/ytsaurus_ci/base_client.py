import logging
from functools import lru_cache

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30


class RegistryClientBase:
    def __init__(self, base_url: str, headers: dict, max_retries: int = 3, backoff_factor: float = 1.0):
        self._base_url = base_url
        self.session = requests.Session()
        self.session.headers.update(headers)

        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)

    @lru_cache(maxsize=2048)
    def _make_request(self, method, endpoint, params=None) -> requests.Response:
        url = f"{self._base_url}{endpoint}"
        prepared = self.session.prepare_request(requests.Request(method=method, url=url, params=params))
        settings = self.session.merge_environment_settings(prepared.url, {}, None, None, None)
        logger.info("%s: requesting %s", type(self).__name__, prepared.url)
        response = self.session.send(prepared, timeout=DEFAULT_TIMEOUT, **settings)
        logger.info("%s: %s -> %s", type(self).__name__, prepared.url, response.status_code)
        response.raise_for_status()
        return response
