import logging
from typing import ClassVar

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.core import YtTable
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.diff import DbDiff
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.settings import Settings

from .base import ScenarioBase
from .helpers import check_and_log_db_diff
from .helpers import ensure_collocation
from .helpers import ensure_db_consumers
from .helpers import UnmanagedDatabaseBuilder
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
        self.has_diff |= check_result.has_diff

        if self.settings.ensure_collocation:
            self.has_diff |= ensure_collocation(
                self.desired.main, self.actual.main, self.yt_client_factory, self.settings, log_only=True
            )
        self.has_diff |= ensure_db_consumers(self.desired, self.actual, self.yt_client_factory, log_only=True)

        if self.settings.managed_roots:
            unmanaged_db = UnmanagedDatabaseBuilder(
                self.desired, self.actual, self.yt_client_factory, self.settings
            ).build()
            has_unmanaged_items = False
            for cluster in unmanaged_db.all_clusters:
                items = sorted(list(cluster.nodes.values()) + list(cluster.tables.values()), key=lambda x: x.path)
                for item in items:
                    if isinstance(item, YtTable):
                        LOG.warning("Unmanaged %s: %s", item.table_type, item.rich_path)
                    else:
                        LOG.warning("Unmanaged %s: %s", item.readable_node_type, item.rich_path)
                has_unmanaged_items |= bool(items)
            if has_unmanaged_items:
                LOG.warning("Consider running 'drop_unmanaged' scenario.")

        assert check_result.is_valid, "Incompatible changes found!"

        return []
