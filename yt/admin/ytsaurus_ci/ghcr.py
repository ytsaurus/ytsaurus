import logging
from dataclasses import dataclass

import frozendict

from yt.admin.ytsaurus_ci.base_client import RegistryClientBase

logger = logging.getLogger(__name__)


@dataclass
class GitHubAuth:
    token: str
    base_url: str


class GitHubPackagesClient(RegistryClientBase):
    def __init__(self, auth: GitHubAuth, max_retries: int = 3, backoff_factor: float = 1.0):
        self.config = auth
        super().__init__(
            auth.base_url,
            {"Accept": "application/vnd.github+json", "Authorization": f"Bearer {auth.token}"},
            max_retries,
            backoff_factor,
        )

    def get_commit_info(self, org: str, repo: str, commit_hash: str):
        endpoint = f"/repos/{org}/{repo}/commits/{commit_hash}"
        response = self._make_request("GET", endpoint)
        return response.json()

    def get_package_versions(self, org: str, package_name, package_type="container"):
        endpoint = f"/orgs/{org}/packages/{package_type}/{package_name}/versions"
        params = {}
        params.setdefault("per_page", 100)
        params.setdefault("page", 1)

        while True:
            response = self._make_request("GET", endpoint, params=frozendict.frozendict(params))
            data = response.json()

            if not data:
                logger.info("ghcr: %s/%s: no more versions, stopping at page %s", org, package_name, params["page"])
                break

            logger.info("ghcr: %s/%s: got %s version(s) on page %s", org, package_name, len(data), params["page"])
            for version in data:
                yield version

            if 'next' not in response.links:
                break

            params["page"] += 1

        return []
