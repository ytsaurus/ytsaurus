from typing import Callable
from dataclasses import dataclass, asdict, field, fields
from typing import Dict, List, Optional, Any


@dataclass
class ComponentVersion:
    version_filter: Optional[str]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ComponentVersion':
        return cls(version_filter=data.get('version_filter'))


@dataclass
class CheckDescription:
    helmUrl: str
    releaseName: str
    version: str

    def dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CheckDescription':
        return cls(helmUrl=data['helmUrl'], releaseName=data['releaseName'], version=data['version'])


@dataclass
class Check:
    type: str
    description: CheckDescription

    def dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Check':
        return cls(type=data['type'], description=CheckDescription.from_dict(data['description']))


@dataclass(frozen=True)
class Scenario:
    type: str
    k8s_template: str
    checks: List[str]
    ttl: int
    operator: Optional[ComponentVersion] = None
    components: Dict[str, ComponentVersion] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Scenario':
        return cls(
            type=data['type'],
            k8s_template=data['k8s_template'],
            checks=data['checks'],
            ttl=data['ttl'],
            operator=ComponentVersion.from_dict(data['operator']) if data.get('operator') else None,
            components={name: ComponentVersion.from_dict(comp) for name, comp in data.get('components', {}).items()},
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ScenarioUpgrade(Scenario):
    upgrade_config: str = ""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ScenarioUpgrade':
        base = Scenario.from_dict(data)
        return cls(
            **{f.name: getattr(base, f.name) for f in fields(Scenario)},
            upgrade_config=data['upgrade_config'],
        )


@dataclass
class ScenarioConfig:
    _default_deserializer = Scenario.from_dict
    _special_deserializers = {
        'upgrade': ScenarioUpgrade.from_dict,
    }
    scenarios: Dict[str, Scenario]
    checks: Dict[str, Check]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ScenarioConfig':
        return cls(
            scenarios={name: cls._deserializer(scenario)(scenario) for name, scenario in data['scenarios'].items()},
            checks={name: Check.from_dict(check) for name, check in data['checks'].items()},
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def _deserializer(cls, scenario: Dict[str, Any]) -> Callable[[Dict[str, Any]], Scenario]:
        return cls._special_deserializers.get(scenario.get('type'), cls._default_deserializer)
