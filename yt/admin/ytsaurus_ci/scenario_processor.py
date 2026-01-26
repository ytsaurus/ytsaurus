import json
import os
import re
from abc import ABC, abstractmethod
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import List

import yaml
from jinja2 import Environment, FileSystemLoader

from library.python import resource
from yt.admin.ytsaurus_ci import check_registry
from yt.admin.ytsaurus_ci import compatibility_graph
from yt.admin.ytsaurus_ci import component_registry
from yt.admin.ytsaurus_ci import consts
from yt.admin.ytsaurus_ci import ghcr
from yt.admin.ytsaurus_ci import models


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_PATH = os.path.join(BASE_DIR, "templates")

SCENARIOS_FILE_PATH = "configs/scenarios.yaml"


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


class TaskCI:
    def __init__(
        self,
        cluster_components,
        scenario_config: models.Scenario,
        component_registry: component_registry.VersionComponentRegistry,
        ghcr_client: ghcr.GitHubPackagesClient,
        scenario_name: str,
        tests: List[models.Check],
    ):
        self._scenario_name = scenario_name
        self._k8s_template_path = scenario_config.k8s_template
        self._task_ttl = scenario_config.ttl

        operator_version = cluster_components["operator"]
        self._operator = OperatorComponent(
            source=component_registry.get_origin("operator", operator_version),
            version=operator_version,
            ghcr_client=ghcr_client,
        )

        self._components = [
            ClusterComponent(
                component_name,
                component_registry.get_origin(component_name, cluster_components[component_name]),
                cluster_components[component_name],
                ghcr_client,
            )
            for component_name in scenario_config.components
        ]

        self._tests = tests

    def _render_manifest(self) -> str:
        template_values = {
            "images": {},
            "cluster_name": "ytsaurus-ci",
        }
        for component in self._components:
            template_values["images"][component.name] = component.image

        env = Environment(loader=FileSystemLoader(TEMPLATES_PATH))
        template = env.get_template(self._k8s_template_path)

        return template.render(**template_values)

    def preview(self):
        return json.dumps(
            [comp.to_dict() for comp in self._components] + [self._operator.to_dict()],
            indent=2,
            ensure_ascii=False,
        )

    def to_dict(self):
        if not self._components:
            raise Exception("Scenario without at least one component is invalid")

        result = {
            "checks": self._tests,
            "components": [component.to_dict() for component in self._components],
            "k8sSpec": self._render_manifest(),
            "operator": self._operator.to_dict(),
            "scenario": self._scenario_name,
            "ttl": self._task_ttl,
        }

        return result


def _make_task(args):
    scenario_name = args["scenario_name"]
    config = args["config"]
    check_registry = args["check_registry"]

    return TaskCI(
        cluster_components=args["cluster_components"],
        scenario_config=config,
        component_registry=args["registry"],
        ghcr_client=args["ghcr_client"],
        scenario_name=f"{scenario_name}/{config.type}",
        tests=[check_registry.get_check(test_name).dict() for test_name in config.checks],
    )


def _make_tasks(
    config: models.Scenario,
    ghcr_client: ghcr.GitHubPackagesClient,
    scenario_name: str,
    check_registry: check_registry.CheckRegistry,
    version_filter: dict,
) -> List[TaskCI]:
    for component, constraint in version_filter.items():
        if component == "operator":
            continue
        if component not in config.components:
            raise ValueError("unexpected component, add it to the scenario", component)
        if config.components[component].version_filter:
            raise ValueError(
                "it is forbidden to override the filter from the scenario",
                component,
                constraint,
                config.components[component],
            )
        if not constraint:
            raise ValueError("filter cannot be empty")
        config.components[component].version_filter = constraint

    constraints = {
        name: component.version_filter for name, component in config.components.items() if component.version_filter
    }
    for constraint in (
        config.operator.version_filter,
        version_filter.get("operator"),
    ):
        if not constraint:
            continue

        if constraints.get("operator"):
            raise ValueError("it is forbidden to override the operator's filter from the scenario", constraint)

        constraints["operator"] = constraint

    registry = component_registry.VersionComponentRegistry(yaml.safe_load(resource.resfs_read(consts.COMPONENTS_PATH)))
    graph = compatibility_graph.CompatibilityGraph(registry)
    paths = graph.find_all_test_suites(constraints)

    tasks_kwargs_for_futures = [
        {
            "cluster_components": cluster_components,
            "config": config,
            "registry": registry,
            "ghcr_client": ghcr_client,
            "scenario_name": scenario_name,
            "check_registry": check_registry,
        }
        for cluster_components in paths
    ]

    with ThreadPoolExecutor() as pool:
        return list(pool.map(_make_task, tasks_kwargs_for_futures))

    return []


def ProcessScenario(scenario_name: str, auth: ghcr.GitHubAuth, version_filter: dict = {}) -> List[TaskCI]:
    ghcr_client = ghcr.GitHubPackagesClient(auth)

    scenario_config = models.ScenarioConfig.from_dict(yaml.safe_load(resource.resfs_read(SCENARIOS_FILE_PATH)))
    if scenario_name not in scenario_config.scenarios:
        raise Exception(f"Scenario {scenario_name} not found")

    test_registry = check_registry.CheckRegistry(scenario_config.checks)

    return _make_tasks(
        scenario_config.scenarios[scenario_name],
        ghcr_client,
        scenario_name,
        test_registry,
        version_filter,
    )
