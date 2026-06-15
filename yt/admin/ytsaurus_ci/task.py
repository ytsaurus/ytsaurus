from abc import ABC, abstractmethod
import json
import os
from typing import Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from yt.admin.ytsaurus_ci import component_registry
from yt.admin.ytsaurus_ci import ghcr
from yt.admin.ytsaurus_ci import models
from yt.admin.ytsaurus_ci.components import ClusterComponent, OperatorComponent

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATES_PATH = os.path.join(BASE_DIR, "templates")


class TaskCI(ABC):
    def __init__(
        self,
        cluster_components: Dict[str, str],
        scenario_config: models.Scenario,
        version_registry: component_registry.VersionComponentRegistry,
        ghcr_client: ghcr.GitHubPackagesClient,
        scenario_name: str,
        tests: List[models.Check],
        templates_path: Optional[str] = None,
    ) -> None:
        self._scenario_name = scenario_name
        self._k8s_template_path = scenario_config.k8s_template
        self._task_ttl = scenario_config.ttl
        self._templates_path = templates_path or TEMPLATES_PATH
        operator_version = cluster_components["operator"]
        self._operator = OperatorComponent(
            source=version_registry.get_origin("operator", operator_version),
            version=operator_version,
            ghcr_client=ghcr_client,
        )

        component_names = (
            list(scenario_config.components.keys())
            if scenario_config.components
            else [name for name in cluster_components if name != "operator"]
        )
        self._components = [
            ClusterComponent(
                name,
                version_registry.get_origin(name, cluster_components[name]),
                cluster_components[name],
                ghcr_client,
            )
            for name in component_names
        ]
        self._tests = tests

    def preview(self) -> str:
        return json.dumps(
            [component.to_dict() for component in self._components] + [self._operator.to_dict()],
            indent=2,
            ensure_ascii=False,
        )

    def _render_manifest(self, components: List[ClusterComponent]) -> str:
        template_values = {
            "images": {component.name: component.image for component in components},
            "cluster_name": "ytsaurus-ci",
        }
        env = Environment(loader=FileSystemLoader(self._templates_path))
        template = env.get_template(self._k8s_template_path)
        return template.render(**template_values)

    def _base_payload(self) -> dict:
        if not self._components:
            raise ValueError("Scenario without at least one component is invalid")

        return {
            "checks": [test.dict() for test in self._tests],
            "components": [component.to_dict() for component in self._components],
            "k8sSpec": self._render_manifest(self._components),
            "operator": self._operator.to_dict(),
            "scenario": self._scenario_name,
            "ttl": self._task_ttl,
        }

    @abstractmethod
    def to_dict(self) -> dict:
        raise NotImplementedError()


class DefaultTaskCI(TaskCI):
    def to_dict(self) -> dict:
        return self._base_payload()


class UpgradeTaskCI(TaskCI):
    def __init__(
        self,
        cluster_components: Dict[str, str],
        scenario_config: models.Scenario,
        version_registry: component_registry.VersionComponentRegistry,
        ghcr_client: ghcr.GitHubPackagesClient,
        scenario_name: str,
        tests: List[models.Check],
        upgrade_to: Optional[Dict[str, str]] = None,
        upgrade_config_name: Optional[str] = None,
        templates_path: Optional[str] = None,
    ) -> None:
        super().__init__(
            cluster_components,
            scenario_config,
            version_registry,
            ghcr_client,
            scenario_name,
            tests,
            templates_path,
        )

        self._upgrade_config_name = upgrade_config_name
        self._upgrade_to = upgrade_to or {}
        self._upgrade_operator = None
        self._upgrade_components = list(self._components)
        if self._upgrade_to:
            upgrade_operator_version = self._upgrade_to.get("operator")
            if upgrade_operator_version:
                self._upgrade_operator = OperatorComponent(
                    source=version_registry.get_origin("operator", upgrade_operator_version),
                    version=upgrade_operator_version,
                    ghcr_client=ghcr_client,
                )

            self._upgrade_components = []
            for component in self._components:
                upgrade_version = self._upgrade_to.get(component.name)
                if not upgrade_version:
                    self._upgrade_components.append(component)
                    continue

                self._upgrade_components.append(
                    ClusterComponent(
                        component.name,
                        version_registry.get_origin(component.name, upgrade_version),
                        upgrade_version,
                        ghcr_client,
                    )
                )

    def to_dict(self) -> dict:
        result = self._base_payload()
        if self._upgrade_to:
            upgrade_config = {
                "targetK8sSpec": self._render_manifest(self._upgrade_components),
            }
            if self._upgrade_operator:
                upgrade_config["upgradeOperator"] = self._upgrade_operator.to_dict()
            if self._upgrade_config_name:
                upgrade_config["scenarioName"] = self._upgrade_config_name
            result["upgradeConfig"] = upgrade_config

        return result
