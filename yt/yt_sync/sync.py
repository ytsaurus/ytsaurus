import logging
import time

import yt.wrapper as yt
from yt.yt_sync.core import DefaultTableFilter
from yt.yt_sync.core import Settings
from yt.yt_sync.core import TableFilterBase
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtColumn
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.spec import Consumer
from yt.yt_sync.core.spec import Node
from yt.yt_sync.core.spec import Table
from yt.yt_sync.core.state_builder import ActualStateBuilder
from yt.yt_sync.core.state_builder import DesiredStateBuilder
from yt.yt_sync.core.state_builder import DesiredStateBuilderBase
from yt.yt_sync.core.state_builder import DesiredStateBuilderDeprecated
from yt.yt_sync.core.state_builder import FakeActualStateBuilder
from yt.yt_sync.scenario import DumpSpecScenario
from yt.yt_sync.scenario import get_scenario_type
from yt.yt_sync.scenario import list_known_scenarios

from .lock import DbLockBase

LOG = logging.getLogger("yt_sync")


class YtSync:
    def __init__(
        self,
        yt_client_factory: YtClientFactory,
        settings: Settings,
        lock: DbLockBase,
        scenario_name: str,
        table_filter: TableFilterBase = DefaultTableFilter(),
    ):
        self._yt_client_factory: YtClientFactory = yt_client_factory
        self._settings: Settings = settings
        self._lock = lock
        assert scenario_name in set(list_known_scenarios()), f"Unknown scenario {scenario_name}"
        self._scenario_name: str = scenario_name
        self._table_filter: TableFilterBase = table_filter

        is_chaos = Settings.CHAOS_DB == self._settings.db_type
        self.desired: YtDatabase = YtDatabase(is_chaos=is_chaos)
        self.actual: YtDatabase = YtDatabase(is_chaos=is_chaos)

        desired_state_builder_type = (
            DesiredStateBuilderDeprecated if settings.use_deprecated_spec_format else DesiredStateBuilder
        )
        self._desired_state_builder: DesiredStateBuilderBase = desired_state_builder_type(
            self._yt_client_factory, self.desired, is_chaos, self._table_filter
        )

    def add_desired_node(self, node_settings: Types.Attributes | Node):
        self._desired_state_builder.add_node(node_settings)

    def add_desired_table(self, table_settings: Types.Attributes | Table):
        self._desired_state_builder.add_table(table_settings)

    def add_desired_consumer(self, consumer_settings: Types.Attributes | Consumer):
        if self._settings.ensure_native_queue_consumers:
            self._desired_state_builder.add_consumer(consumer_settings)

    def sync(self, *args, **kwargs):
        self._settings.is_ok()  # check settings is ok
        LOG.info("YT Sync started, scenario=%s, dry=%s", self._scenario_name, self._yt_client_factory.is_dry_run())
        try:
            self._desired_state_builder.finalize(self._settings)
            self._load_actual_state()
            with self._lock.lock():
                self._run_scenario(*args, **kwargs)
        finally:
            LOG.info("YT Sync finished")

    def _load_actual_state(self):
        LOG.info("Load actual DB state")
        builder = ActualStateBuilder(self._settings, self._yt_client_factory, self._table_filter)
        if self._settings.use_fake_actual_state_builder or self._scenario_name == DumpSpecScenario.SCENARIO_NAME:
            LOG.info("Use fake actual state builder")
            builder = FakeActualStateBuilder(self._table_filter)

        retries = 5
        for i in range(retries):
            try_num = i + 1
            LOG.debug("Build actual state, try #%s", try_num)
            try:
                self.actual = builder.build_from(self.desired)
                LOG.debug("Build actual state is successful")
                break
            except (AssertionError, yt.YtResponseError) as e:
                if try_num < retries:
                    wait_time = try_num * 2
                    LOG.debug("Build actual state failed with error, next try in %s seconds...", wait_time, exc_info=e)
                    time.sleep(wait_time)
                else:
                    raise e

    def _run_scenario(self, *args, **kwargs):
        scenario_type = get_scenario_type(self._scenario_name)
        scenario = scenario_type(self.desired, self.actual, self._settings, self._yt_client_factory)
        scenario.setup(*args, **kwargs)
        LOG.info("Run YT Sync scenario %s", self._scenario_name)
        scenario.run()

    @staticmethod
    def disable_normalized_expression():
        # Disables normalized_expression in schema comparision.
        # Should be tested with True for all tables, then removed completely with normalized expression.
        # See YTADMINREQ-43528 for details.
        YtColumn.DISABLE_NORMALIZED_EXPRESSION = True
