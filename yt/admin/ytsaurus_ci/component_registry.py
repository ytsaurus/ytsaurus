from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

import yaml

from library.python import resource
from yt.admin.ytsaurus_ci import consts

KNOWN_REGISTRIES = {"ghcr", "yandex_cr"}
DEFAULT_GRAPH = "default"


def _read_internal_components_data() -> Optional[bytes]:
    try:
        return resource.resfs_read(consts.COMPONENTS_INTERNAL_PATH)
    except KeyError:
        return None


def has_internal_components() -> bool:
    return _read_internal_components_data() is not None


def load_components_config() -> Dict:
    config = yaml.safe_load(resource.resfs_read(consts.COMPONENTS_PATH))

    internal_data = _read_internal_components_data()
    if internal_data:
        config.update(yaml.safe_load(internal_data))

    return config


@dataclass(frozen=True)
class Source:
    repo: str
    container: str
    image_tag: str
    org: str
    registry: str = "ghcr"


@dataclass(frozen=True)
class VersionComponent:
    name: str
    version: str
    source: Source
    constraints: Optional[Dict[str, str]]
    show_in_matrix: bool


class VersionComponentRegistry:
    def __init__(self, components_config: Dict):
        self._components: Dict[str, Dict[str, VersionComponent]] = defaultdict(dict)
        self._component_names: Dict[str, str] = {}
        self._component_graphs: Dict[str, Set[str]] = {}

        for component, config in components_config.items():
            self._component_names[component] = config.get("component_name", component)
            self._component_graphs[component] = set(config.get("graphs", [DEFAULT_GRAPH]))

            requirements_data = resource.resfs_read(config["requirements"])
            if requirements_data is None:
                raise ValueError(
                    f"Requirements resource {config['requirements']!r} for component {component!r} is not available"
                )

            compat = yaml.safe_load(requirements_data)
            if not compat:
                raise ValueError("Compat config cannot be empty")

            origins = config["origins"]
            for version, settings in compat.items():
                version = str(version)
                source_name = settings["source"]
                if source_name not in origins:
                    raise ValueError(f"Version {version} of component {component} is not registered")

                origin = origins[source_name]
                registry_type = origin.get("registry", "ghcr")
                if registry_type not in KNOWN_REGISTRIES:
                    raise ValueError(f"Unknown registry {registry_type!r} for {component}/{source_name}")

                self._components[component][version] = VersionComponent(
                    name=component,
                    version=version,
                    source=Source(
                        repo=origin["repo"],
                        container=origin["container"],
                        image_tag=origin["image_tag"],
                        org=origin["org"],
                        registry=registry_type,
                    ),
                    constraints=settings.get("constraints"),
                    show_in_matrix=settings.get("show_in_matrix", True),
                )

        self._validate_constraints()

    def _validate_constraints(self):
        for component, versions in self._components.items():
            for version_component in versions.values():
                for constrained in version_component.constraints or {}:
                    if constrained not in self._components:
                        raise ValueError(
                            f"Unknown component {constrained!r} in constraints of {component}:{version_component.version}"
                        )
                    if not (self._component_graphs[component] & self._component_graphs[constrained]):
                        raise ValueError(
                            f"Component {constrained!r} in constraints of "
                            f"{component}:{version_component.version} shares no graph with {component!r}"
                        )

    def get_components(self):
        return sorted(self._components.keys())

    def get_component_versions(self, name):
        versions = self._components.get(name)
        if versions:
            return versions.keys()

        raise ValueError(f"Versions list for {name} is empty")

    def get_component(self, name, version):
        component = self._components.get(name, {}).get(version)
        if component:
            return component

        raise ValueError(f"Not described {name}:{version}")

    def get_origin(self, name, version):
        return self.get_component(name, version).source

    def get_component_name(self, name):
        return self._component_names.get(name, name)

    def get_graphs(self) -> List[str]:
        graphs = set()
        for component_graphs in self._component_graphs.values():
            graphs |= component_graphs

        return sorted(graphs)

    def get_components_in_graph(self, graph: str = DEFAULT_GRAPH) -> List[str]:
        return sorted(name for name, graphs in self._component_graphs.items() if graph in graphs)

    def get_constraints(self, name, version):
        return self.get_component(name, version).constraints

    def get_matrix_versions(self, name):
        versions = self._components.get(name)
        if versions:
            return [v for v, c in versions.items() if c.show_in_matrix]

        raise ValueError(f"Versions list for {name} is empty")
