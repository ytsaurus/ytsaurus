from typing import ClassVar

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.settings import Settings

from .base import ScenarioBase
from .registry import scenario


@scenario
class DummyScenario(ScenarioBase):
    SCENARIO_NAME: ClassVar[str] = "dummy"
    SCENARIO_DESCRIPTION: ClassVar[str] = "Do nothing"

    def __init__(
        self,
        desired: YtDatabase,
        actual: YtDatabase,
        settings: Settings,
        yt_client_factory: YtClientFactory,
    ):
        super().__init__(desired, actual, settings, yt_client_factory)

    def generate_actions(self) -> list[ActionBatch]:
        return []
