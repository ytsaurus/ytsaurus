from dataclasses import dataclass

import curlify
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass
class YCFunctionAuth:
    cloud_function_token: str
    cloud_function_url: str = "https://functions.yandexcloud.net"


class CloudFunctionClient:
    SUBMIT_TASK_ID = "d4ee6v3cr3udu6bpnova"
    RUN_TASK_ID = "d4ei35u5bejcoiccbkcf"

    def __init__(self, auth: YCFunctionAuth, max_retries: int = 3, backoff_factor: int = 1.0):
        self._base_url = auth.cloud_function_url
        self._session = requests.Session()
        self._session.headers.update(
            {"Authorization": f"Api-Key {auth.cloud_function_token}"},
        )
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[504],
            allowed_methods=["POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("https://", adapter)

    def _prepare(self, payload, cloud_function_id, method):
        return self._session.prepare_request(
            requests.Request(
                method.upper(),
                f"{self._base_url}/{cloud_function_id}?integration=raw",
                json=payload,
            )
        )

    def submit_task(self, payload, apply):
        req = self._prepare(payload, self.SUBMIT_TASK_ID, "post")
        if not apply:
            return curlify.to_curl(req)
        response = self._session.send(req)
        response.raise_for_status()

        return response.json()

    def run_task(self, job_id):
        payload = {"job_id": job_id}
        req = self._prepare(payload, self.RUN_TASK_ID, "post")
        response = self._session.send(req)
        response.raise_for_status()

        return response.json()
