from typing import ClassVar

import pytest

from yt.yt_sync.scenario.base import ScenarioBase
from yt.yt_sync.scenario.registry import get_scenario_type, list_known_scenarios, scenario


def test_get_not_registered_scenario():
    with pytest.raises(AssertionError):
        get_scenario_type("_unknown_")


@scenario
class DummyTestScenario(ScenarioBase):
    SCENARIO_NAME: ClassVar[str] = "test_dummy"


def test_registered_scenario():
    assert "test_dummy" in set(list_known_scenarios())
    scenario_type = get_scenario_type("test_dummy")
    assert DummyTestScenario == scenario_type
