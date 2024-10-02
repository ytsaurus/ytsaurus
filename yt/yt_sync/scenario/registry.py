from typing import Type

from .base import ScenarioBase

_SCENARIOS: dict[str, Type[ScenarioBase]] = dict()


def register_scenario_type(name: str, scenario_class: Type[ScenarioBase]):
    assert name not in _SCENARIOS, "Duplicate scenario name"
    _SCENARIOS[name] = scenario_class


def scenario(scenario_class: Type[ScenarioBase]) -> Type[ScenarioBase]:
    assert issubclass(scenario_class, ScenarioBase)
    register_scenario_type(scenario_class.SCENARIO_NAME, scenario_class)
    return scenario_class


def get_scenario_type(name: str) -> Type[ScenarioBase]:
    assert name in _SCENARIOS, f"Unknown scenario {name}"
    return _SCENARIOS[name]


def list_known_scenarios() -> dict[str, Type[ScenarioBase]]:
    return _SCENARIOS
