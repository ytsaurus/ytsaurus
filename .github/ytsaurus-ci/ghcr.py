import requests
from typing import Optional, Dict, Any, List
from dataclasses import dataclass


@dataclass
class GitHubAuth:
    token: str
    base_url: str
    org: str = 'ytsaurus'


class GitHubPackagesClient:
    def __init__(self, auth: GitHubAuth):
        self.config = auth
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.config.token}"
        })

    def _make_request(
        self,
        method,
        endpoint,
        params = None
    ) -> requests.Response:
        url = f"{self.config.base_url}{endpoint}"
        response = self.session.request(method=method, url=url, params=params)
        response.raise_for_status()
        return response

    def get_commit_info(self, repo: str, commit_hash: str):
        endpoint = f"/repos/{self.config.org}/{repo}/commits/{commit_hash}"
        response = self._make_request("GET", endpoint)
        return response.json()

    def get_package_versions(self, package_name, package_type = "container"):
        org = self.config.org
        endpoint = (
            f"/orgs/{org}/packages/{package_type}/{package_name}/versions"
        )
        params = {"per_page": 100}
        response = self._make_request("GET", endpoint, params=params)
        return response.json()
