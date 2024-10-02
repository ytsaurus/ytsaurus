import logging
from typing import ClassVar

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.diff import DbDiff
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.settings import Settings

from .base import ScenarioBase
from .helpers import check_and_log_db_diff
from .helpers import ensure_collocation
from .helpers import ensure_db_consumers
from .registry import scenario

LOG = logging.getLogger("yt_sync")


@scenario
class DumpDiffScenario(ScenarioBase):
    SCENARIO_NAME: ClassVar[str] = "dump_diff"
    SCENARIO_DESCRIPTION: ClassVar[str] = "Dump diff only"

    def __init__(
        self,
        expected: YtDatabase,
        actual: YtDatabase,
        settings: Settings,
        yt_client_factory: YtClientFactory,
    ):
        super().__init__(expected, actual, settings, yt_client_factory)

    def generate_actions(self) -> list[ActionBatch]:
        if not self.desired.clusters:
            LOG.info("Empty database, nothing to compare.")
            return []

        diff = DbDiff.generate(self.settings, self.desired, self.actual)
        check_result = check_and_log_db_diff(self.settings, self.desired, diff, add_prompt=True)

        if self.settings.ensure_collocation:
            ensure_collocation(
                self.desired.main, self.actual.main, self.yt_client_factory, self.settings, log_only=True
            )
        if self.settings.ensure_native_queue_consumers:
            ensure_db_consumers(self.desired, self.actual, self.yt_client_factory, log_only=True)

        assert check_result, "Incompatible changes found!"

        return []
