import yt.logger as logger
import yt.packages.requests as requests

from typing import Any, Dict, Optional, Tuple
from urllib.parse import urljoin
import os


class PrometheusClient:
    def __init__(self, url: str):
        self.url = url if url.endswith("/") else url + "/"
        self._auth = self._auth_from_env()
        if self._auth:
            logger.info("Using Basic Auth from PROMETHEUS_USER/PROMETHEUS_PASSWORD")

    @staticmethod
    def _auth_from_env() -> Optional[Tuple[str, str]]:
        user = os.environ.get("PROMETHEUS_USER")
        password = os.environ.get("PROMETHEUS_PASSWORD")
        return (user, password) if user and password else None

    def _get(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        full = urljoin(self.url, endpoint.lstrip("/"))
        response = requests.get(full, params=params, auth=self._auth, timeout=60)
        response.raise_for_status()
        return response.json()

    def count_series(self, selector: str, at: float) -> int:
        try:
            data = self._get("api/v1/query", {"query": f"count({selector})", "time": at})
        except requests.exceptions.RequestException as e:
            logger.warning(f"Could not estimate {selector}: {type(e).__name__}: {e}")
            return -1
        try:
            result = data.get("data", {}).get("result", [])
            return int(float(result[0]["value"][1])) if result else 0
        except (KeyError, IndexError, ValueError, TypeError) as e:
            logger.warning(f"Unexpected response for count({selector}): {type(e).__name__}: {e}")
            return -1

    def query_range(self, selector: str, start: float, end: float, step: str) -> Dict[str, Any]:
        return self._get(
            "api/v1/query_range",
            {"query": selector, "start": start, "end": end, "step": step},
        )
