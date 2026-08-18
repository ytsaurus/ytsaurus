import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict

from yt.admin.ytsaurus_ci import component_registry

logger = logging.getLogger(__name__)


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
        clients: Dict[str, object],
        render_name: str = None,
    ):
        self._name = name
        self._render_name = render_name or name
        self._container = source.container
        self._repo = source.repo
        self._org = source.org
        self._registry = source.registry
        self._version = None

        image_template = source.image_tag.replace("{{ version }}", version)
        self._image_regexp = re.compile(image_template)

        self._image_tag = None
        self._revision = None
        self._commit_date = None
        self._branch = None

        self._clients = clients

        self._prepare()

    @property
    def name(self):
        return self._name

    @property
    def render_name(self):
        return self._render_name

    def _client(self):
        client = self._clients.get(self._registry)
        if not client:
            raise ValueError(f"No client configured for registry {self._registry!r}")

        return client

    def _iter_images_with_tags(self):
        client = self._client()
        if self._registry == "ghcr":
            for image in client.get_package_versions(self._org, self._container):
                yield image["metadata"]["container"]["tags"]
        elif self._registry == "yandex_cr":
            for image in client.get_image_tags(self._org, f"{self._repo}/{self._container}"):
                yield image.get("tags") or []
        else:
            raise ValueError(f"Unknown registry {self._registry!r}")

    def _prepare(self):
        if not self._image_regexp:
            raise ValueError("Regexp invalid or not set")

        logger.info(
            "%s: searching registry=%s org=%s repo=%s container=%s for tag matching %s",
            self._name,
            self._registry,
            self._org,
            self._repo,
            self._container,
            self._image_regexp.pattern,
        )

        for tags in self._iter_images_with_tags():
            for tag in tags:
                match = self._image_regexp.match(tag)
                if not match:
                    logger.info("%s: tag %s did not match %s", self._name, tag, self._image_regexp.pattern)
                    continue

                self._image_tag = tag
                extra_info_from_tag = match.groupdict()
                if extra_info_from_tag:
                    self._version = extra_info_from_tag.get("version")
                    self._revision = extra_info_from_tag.get("commit_hash")
                    if self._revision and self._registry == "ghcr":
                        commit_info = self._client().get_commit_info(
                            self._org,
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
            logger.info("%s: no tag matched %s", self._name, self._image_regexp.pattern)
            raise Exception(f"No one tag does not match with {self._image_regexp}")

        logger.info(
            "%s: matched tag=%s version=%s revision=%s", self._name, self._image_tag, self._version, self._revision
        )

    @property
    def image(self):
        if None in (self._repo, self._name, self._image_tag):
            raise Exception(f"ClusterComponent {self._name} is not prepared")

        if self._registry == "ghcr":
            return f"ghcr.io/{self._org}/{self._container}:{self._image_tag}"

        return f"cr.yandex/{self._org}/{self._repo}/{self._container}:{self._image_tag}"

    @abstractmethod
    def to_dict(self):
        raise NotImplementedError()


class ClusterComponent(Component):
    def to_dict(self):
        return {
            "branch": self._branch,
            "revision": self._revision,
            "commitDate": self._commit_date,
            "name": self._render_name.upper(),
            "version": self._version,
        }


class OperatorComponent(ClusterComponent):
    def __init__(self, source, version, clients):
        if source.registry != "ghcr":
            raise ValueError(f"Unsupported registry for operator: {source.registry!r}")

        super().__init__("operator", source, version, clients)

    @property
    def image(self):
        return f"ghcr.io/{self._org}/{self._container}"

    def to_dict(self):
        return {
            "helmUrl": self.image,
            "operator": super().to_dict(),
        }
