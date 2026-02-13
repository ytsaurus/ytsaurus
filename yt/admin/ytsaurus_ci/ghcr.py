from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass
class GitHubAuth:
    token: str
    base_url: str
    org: str = 'ytsaurus'


class GitHubPackagesClient:
    def __init__(self, auth: GitHubAuth, max_retries: int = 3, backoff_factor: float = 1.0):
        self.config = auth
        self.session = requests.Session()
        self.session.headers.update(
            {"Accept": "application/vnd.github+json", "Authorization": f"Bearer {self.config.token}"},
        )

        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)

    def _make_request(self, method, endpoint, params=None) -> requests.Response:
        url = f"{self.config.base_url}{endpoint}"
        response = self.session.request(method=method, url=url, params=params)
        response.raise_for_status()
        return response

    def get_commit_info(self, repo: str, commit_hash: str):
        endpoint = f"/repos/{self.config.org}/{repo}/commits/{commit_hash}"
        response = self._make_request("GET", endpoint)
        return response.json()

    def get_package_versions(self, package_name, package_type="container"):
        org = self.config.org
        endpoint = f"/orgs/{org}/packages/{package_type}/{package_name}/versions"
        params = {}
        params.setdefault("per_page", 100)
        params.setdefault("page", 1)

        while True:
            response = self._make_request("GET", endpoint, params=params)
            data = response.json()

            if not data:
                break

            for version in data:
                yield version

            if 'next' not in response.links:
                break

            params["page"] += 1

        return []
