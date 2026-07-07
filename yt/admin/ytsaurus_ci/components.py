import re
from abc import ABC, abstractmethod
from datetime import datetime

from yt.admin.ytsaurus_ci import component_registry
from yt.admin.ytsaurus_ci import ghcr


def parse_iso_to_pg_timestamp(date_str: str) -> str:
    if date_str.endswith("Z"):
        date_str = date_str[:-1] + "+00:00"
    dt = datetime.fromisoformat(date_str)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


class Component(ABC):
    def __init__(
        self,
        name: str,
        source: component_registry.Source,
        version: str,
        ghcr_client: ghcr.GitHubPackagesClient,
    ):
        self._name = name
        self._container = source.container
        self._repo = source.repo
        self._version = None

        image_template = source.image_tag.replace("{{ version }}", version)
        self._image_regexp = re.compile(image_template)

        self._image_tag = None
        self._revision = None
        self._commit_date = None
        self._branch = None

        self._ghcr_client = ghcr_client

        self._prepare()

    @property
    def name(self):
        return self._name

    def _prepare(self):
        if not self._image_regexp:
            raise ValueError("Regexp invalid or not set")

        images = self._ghcr_client.get_package_versions(self._container)
        if not images:
            raise Exception(f"Cannot find package {self._container}")

        for image in images:
            for tag in image["metadata"]["container"]["tags"]:
                match = self._image_regexp.match(tag)
                if match:
                    self._image_tag = tag
                    extra_info_from_tag = match.groupdict()
                    if extra_info_from_tag:
                        self._version = extra_info_from_tag.get("version")
                        self._revision = extra_info_from_tag.get("commit_hash")
                        if self._revision:
                            commit_info = self._ghcr_client.get_commit_info(
                                self._repo,
                                self._revision,
                            )
                            commit_date = parse_iso_to_pg_timestamp(commit_info["commit"]["author"]["date"])
                            self._commit_date = commit_date

                    if not self._version:
                        self._version = match.group()

            if self._image_tag:
                break

        if not self._image_tag:
            raise Exception(f"No one tag does not match with {self._image_regexp}")

    @property
    def image(self):
        if None in (self._repo, self._name, self._image_tag):
            raise Exception(f"ClusterComponent {self._name} is not prepared")

        return f"ghcr.io/{self._repo}/{self._container}:{self._image_tag}"

    @abstractmethod
    def to_dict(self):
        raise NotImplementedError()


class ClusterComponent(Component):
    def to_dict(self):
        return {
            "branch": self._branch,
            "revision": self._revision,
            "commitDate": self._commit_date,
            "name": self._name.upper(),
            "version": self._version,
        }


class OperatorComponent(ClusterComponent):
    def __init__(self, source, version, ghcr_client):
        super().__init__("operator", source, version, ghcr_client)

    @property
    def image(self):
        return f"ghcr.io/ytsaurus/{self._container}"

    def to_dict(self):
        return {
            "helmUrl": self.image,
            "operator": super().to_dict(),
        }
