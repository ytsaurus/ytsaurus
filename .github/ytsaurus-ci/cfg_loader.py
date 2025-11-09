import ghcr
import re
import yaml

from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from datetime import datetime
from jinja2 import Environment, FileSystemLoader


SCENARIOS_PATH = Path("configs/scenarios.yaml")
TEMPLATES_PATH = Path("templates/")

NIGHTLY_IMAGE_TAG_REGEXP = re.compile(
    r"\w+-\d{4}-\d{2}-\d{2}-(?P<commit_hash>\w+)"
)
NIGHTLY_YTOP_IMAGE_TAG_REGEXP = re.compile(
    r"([0-9.]+)-(\w+)-(?P<commit_hash>\w+)"
)

COMPONENTS_WITH_NIGHTLY_TAG = {"ytsaurus", "strawberry", "query_tracker"}
COMPONENTS_WITH_YTOP_TAG = {"operator"}


def parse_iso_to_pg_timestamp(date_str: str) -> str:
    if date_str.endswith("Z"):
        date_str = date_str[:-1] + "+00:00"
    dt = datetime.fromisoformat(date_str)

    return dt.strftime("%Y-%m-%d %H:%M:%S")


class Component:
    def __init__(self, name):
        self._name = name
        self._package = None
        self._version = None
        self._revision = None
        self._commit_date = None
        self._branch = None
        self._image_tag = None
        self._repo = None

    @property
    def name(self):
        return self._name

    @property
    def helm_url(self):
        if not self._package:
            raise Exception("Package not set")

        return f"ghcr.io/ytsaurus/{self._package}"

    @property
    def image(self):
        if None in (self._repo, self._package, self._image_tag):
            raise Exception("Component image information incomplete")

        return f"ghcr.io/{self._repo}/{self._package}:{self._image_tag}"

    def from_dict(self, data):
        self._package = data["package"]
        self._version = data["version"]
        self._repo = data.get("repo", "ytsaurus")

    def enhance_vcs_info(self, ghcr_client: ghcr.GitHubPackagesClient):
        if None in (self._name, self._version):
            raise Exception(
                "Empty component: name or version is missing"
            )

        self._branch = "ref"
        versions = ghcr_client.get_package_versions(self._package)
        if not versions:
            raise Exception(f"Cannot find package {self._package}")

        for version in versions:
            for tag in version["metadata"]["container"]["tags"]:
                if self._version in tag:
                    self._image_tag = tag
                    break
            if self._image_tag:
                break

        if not self._image_tag:
            raise Exception(
                f"Version {self._version} not found for package "
                f"{self._package}"
            )

        match = None
        if self._name in COMPONENTS_WITH_NIGHTLY_TAG:
            match = NIGHTLY_IMAGE_TAG_REGEXP.search(self._image_tag)
        elif self._name in COMPONENTS_WITH_YTOP_TAG:
            match = NIGHTLY_YTOP_IMAGE_TAG_REGEXP.search(self._image_tag)

        if match:
            groups = match.groupdict()
            self._revision = groups["commit_hash"]
            commit_info = ghcr_client.get_commit_info(self._repo, groups["commit_hash"])
            commit_date = parse_iso_to_pg_timestamp(commit_info["commit"]["author"]["date"])
            self._commit_date = commit_date

    def to_json(self):
        jsonify_component = {
            "branch": self._branch,
            "revision": self._revision,
            "commitDate": self._commit_date,
            "name": self._name.upper(),
        }

        if self._name == "operator":
            jsonify_component["version"] = self._image_tag
            jsonify_component = {"operator": jsonify_component}
            jsonify_component["helmUrl"] = self.helm_url
        else:
            jsonify_component["version"] = self._version

        return jsonify_component


class Scenario:
    def __init__(
        self,
        name: str,
        accessible_checks: Dict[str, Any],
        auth: ghcr.GitHubAuth
    ):
        self._ghcr = ghcr.GitHubPackagesClient(auth)
        self._name = name
        self._accessible_checks = accessible_checks
        self._k8s_template_path = None
        self._ttl = 0
        self._operator: Optional[Component] = None
        self._components: List[Component] = []
        self._checks: List[Any] = []

    def from_dict(self, scenario_raw):
        self._k8s_template_path = scenario_raw["k8sTemplate"]
        self._ttl = scenario_raw["ttl"]

        self._operator = Component("operator")
        self._operator.from_dict(scenario_raw["operator"])
        self._operator.enhance_vcs_info(self._ghcr)

        for component_name in scenario_raw["components"]:
            component = Component(component_name)
            component.from_dict(scenario_raw["components"][component_name])
            component.enhance_vcs_info(self._ghcr)
            self._components.append(component)

        if not self._components:
            raise Exception("All components should be described")

        for check in scenario_raw["checks"]:
            check_descriptor = self._accessible_checks.get(check)
            if not check_descriptor:
                raise Exception(f"Check {check} not found")
            self._checks.append(check_descriptor)

    def _render_manifest(self) -> str:
        template_values = {
            "images": {},
            "cluster_name": "ytsaurus-ci",
        }
        for component in self._components:
            template_values["images"][component.name] = component.image

        env = Environment(loader=FileSystemLoader(str(TEMPLATES_PATH)))
        template = env.get_template(self._k8s_template_path)

        return template.render(**template_values)

    def to_json(self):
        if not self._components:
            raise Exception("Scenario without at least one component is invalid")

        result = {
            "ttl": self._ttl,
            "k8sSpec": self._render_manifest(),
            "operator": self._operator.to_json(),
            "components": [component.to_json() for component in self._components],
            "scenario": self._name,
        }

        if self._checks:
            result["checks"] = self._checks

        return result


def ProcessScenario(
    scenario_name: str, auth: ghcr.GitHubAuth
) -> Scenario:
    with open(SCENARIOS_PATH, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
        if not data or "scenarios" not in data:
            raise Exception(f"Invalid scenarios file: {SCENARIOS_PATH}")

        if scenario_name not in data["scenarios"]:
            raise Exception(f"Scenario {scenario_name} not found")

        scenario = Scenario(scenario_name, data.get("checks", {}), auth)
        scenario.from_dict(data["scenarios"][scenario_name])

        return scenario
