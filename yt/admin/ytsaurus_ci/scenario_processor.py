import os
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, List, Optional

import yaml

from library.python import resource
from yt.admin.ytsaurus_ci import check_registry
from yt.admin.ytsaurus_ci import compatibility_graph
from yt.admin.ytsaurus_ci import component_registry
from yt.admin.ytsaurus_ci import consts
from yt.admin.ytsaurus_ci import ghcr
from yt.admin.ytsaurus_ci import models
from yt.admin.ytsaurus_ci.components import ClusterComponent, OperatorComponent  # noqa: re-exported for tests
from yt.admin.ytsaurus_ci.task import DefaultTaskCI, TaskCI, UpgradeTaskCI

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_PATH = os.path.join(BASE_DIR, "templates")
SCENARIOS_FILE_PATH = "configs/scenarios.yaml"


def _make_checks(config: models.Scenario, registry: check_registry.CheckRegistry) -> List[models.Check]:
    return [registry.get_check(name) for name in config.checks]


def _make_task(args: dict) -> DefaultTaskCI:
    config = args["config"]
    return DefaultTaskCI(
        cluster_components=args["cluster_components"],
        scenario_config=config,
        version_registry=args["registry"],
        ghcr_client=args["ghcr_client"],
        scenario_name=f"{args['scenario_name']}/{config.type}",
        tests=_make_checks(config, args["check_registry"]),
        templates_path=args.get("templates_path", TEMPLATES_PATH),
    )


def _make_upgrade_task(args: dict) -> UpgradeTaskCI:
    return UpgradeTaskCI(
        cluster_components=args["cluster_components"],
        scenario_config=args["config"],
        version_registry=args["registry"],
        ghcr_client=args["ghcr_client"],
        scenario_name=f"{args['scenario_name']}/{args['config'].type}",
        tests=_make_checks(args["config"], args["check_registry"]),
        upgrade_to=args.get("upgrade_to"),
        upgrade_config_name=args.get("upgrade_config_name"),
        templates_path=args.get("templates_path", TEMPLATES_PATH),
    )


def _run_parallel(tasks_kwargs: list, mapper: Callable) -> List[TaskCI]:
    with ThreadPoolExecutor() as pool:
        return list(pool.map(mapper, tasks_kwargs))


def _build_constraints(config: models.Scenario, version_filter: Dict[str, str]) -> Dict[str, str]:
    scenario_components = config.components or {}
    constraints = {
        name: component.version_filter for name, component in scenario_components.items() if component.version_filter
    }

    for component, constraint in version_filter.items():
        if component == "operator":
            continue

        if component not in scenario_components:
            raise ValueError("unexpected component, add it to the scenario", component)
        if scenario_components[component].version_filter:
            raise ValueError(
                "it is forbidden to override the filter from the scenario",
                component,
                constraint,
                scenario_components[component],
            )
        if not constraint:
            raise ValueError("filter cannot be empty")

        constraints[component] = constraint

    for constraint in (
        config.operator.version_filter if config.operator else None,
        version_filter.get("operator"),
    ):
        if not constraint:
            continue

        if constraints.get("operator"):
            raise ValueError("it is forbidden to override the operator's filter from the scenario", constraint)

        constraints["operator"] = constraint

    return constraints


def _make_tasks(
    config: models.Scenario,
    ghcr_client: ghcr.GitHubPackagesClient,
    scenario_name: str,
    check_registry: check_registry.CheckRegistry,
    version_filter: Dict[str, str],
) -> List[TaskCI]:
    constraints = _build_constraints(config, version_filter)

    registry = component_registry.VersionComponentRegistry(yaml.safe_load(resource.resfs_read(consts.COMPONENTS_PATH)))
    graph = compatibility_graph.CompatibilityGraph(registry)
    paths = graph.find_all_test_suites(constraints)

    return _run_parallel(
        [
            {
                "cluster_components": cluster_components,
                "config": config,
                "registry": registry,
                "ghcr_client": ghcr_client,
                "scenario_name": scenario_name,
                "check_registry": check_registry,
            }
            for cluster_components in paths
        ],
        _make_task,
    )


def _make_upgrade_tasks(
    config: models.ScenarioUpgrade,
    ghcr_client: ghcr.GitHubPackagesClient,
    scenario_name: str,
    test_registry: check_registry.CheckRegistry,
    upgrade_config: Optional[str],
) -> List[TaskCI]:
    upgrade_cfg = yaml.safe_load(resource.resfs_read(config.upgrade_config))

    if upgrade_config and upgrade_config not in upgrade_cfg:
        available = ", ".join(sorted(upgrade_cfg))
        raise ValueError(f"unknown upgrade config {upgrade_config!r}, available: {available}")

    registry = component_registry.VersionComponentRegistry(yaml.safe_load(resource.resfs_read(consts.COMPONENTS_PATH)))
    graph = compatibility_graph.CompatibilityGraph(registry)

    tasks_kwargs = []
    for name, upgrade in upgrade_cfg.items():
        if upgrade_config and name != upgrade_config:
            continue

        paths = graph.find_all_test_suites(upgrade.get("version_filter", {}))
        tasks_kwargs.extend(
            [
                {
                    "cluster_components": cluster_components,
                    "config": config,
                    "registry": registry,
                    "ghcr_client": ghcr_client,
                    "scenario_name": scenario_name,
                    "check_registry": test_registry,
                    "upgrade_to": upgrade.get("upgrade_to", {}),
                    "upgrade_config_name": name,
                }
                for cluster_components in paths
            ]
        )

    return _run_parallel(tasks_kwargs, _make_upgrade_task)


def ProcessScenario(
    scenario_name: str,
    auth: ghcr.GitHubAuth,
    version_filter: Optional[Dict[str, str]] = None,
    upgrade_config: Optional[str] = None,
) -> List[TaskCI]:
    version_filter = version_filter or {}
    ghcr_client = ghcr.GitHubPackagesClient(auth)

    scenario_config = models.ScenarioConfig.from_dict(yaml.safe_load(resource.resfs_read(SCENARIOS_FILE_PATH)))
    if scenario_name not in scenario_config.scenarios:
        raise Exception(f"Scenario {scenario_name} not found")

    scenario = scenario_config.scenarios[scenario_name]
    test_registry = check_registry.CheckRegistry(scenario_config.checks)

    if scenario.type == "upgrade":
        return _make_upgrade_tasks(scenario, ghcr_client, scenario_name, test_registry, upgrade_config)

    return _make_tasks(scenario, ghcr_client, scenario_name, test_registry, version_filter)
