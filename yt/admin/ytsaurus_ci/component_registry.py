from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Optional

import yaml

from library.python import resource


@dataclass(frozen=True)
class Source:
    repo: str
    container: str
    image_tag: str


@dataclass(frozen=True)
class VersionComponent:
    name: str
    version: str
    source: Source
    constraints: Optional[Dict[str, str]]


class VersionComponentRegistry:
    def __init__(self, components_config: Dict):
        self._components: Dict[str, Dict[str, VersionComponent]] = defaultdict(dict)

        for component, config in components_config.items():
            compat = yaml.safe_load(resource.resfs_read(config["requirements"]))
            if not compat:
                raise ValueError("Compat config cannot be empty")

            origins = config["origins"]
            for version, settings in compat.items():
                version = str(version)
                source = settings["source"]
                if source not in origins:
                    raise ValueError(f"Version {version} of component {component} is not registered")

                self._components[component][version] = VersionComponent(
                    name=component,
                    version=version,
                    source=Source(
                        repo=origins[source]["repo"],
                        container=origins[source]["container"],
                        image_tag=origins[source]["image_tag"],
                    ),
                    constraints=settings.get("constraints"),
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

    def get_constraints(self, name, version):
        return self.get_component(name, version).constraints
