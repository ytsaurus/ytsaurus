import logging

from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtColumn
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.settings import Settings
from yt.yt_sync.core.spec import Consumer
from yt.yt_sync.core.spec import Node
from yt.yt_sync.core.spec import Pipeline
from yt.yt_sync.core.spec import Producer
from yt.yt_sync.core.spec import Table
from yt.yt_sync.core.state_builder import DesiredStateBuilder
from yt.yt_sync.core.state_builder import DesiredStateBuilderBase
from yt.yt_sync.core.state_builder import DesiredStateBuilderDeprecated
from yt.yt_sync.core.state_builder import load_actual_db
from yt.yt_sync.core.table_filter import DefaultTableFilter
from yt.yt_sync.core.table_filter import TableFilterBase
from yt.yt_sync.lock import DbLockBase
from yt.yt_sync.scenario import DumpSpecScenario
from yt.yt_sync.scenario import get_scenario_type
from yt.yt_sync.scenario import list_known_scenarios
from yt.yt_sync.scenario.base import ScenarioBase

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
            self._yt_client_factory,
            self.desired,
            is_chaos,
            self._settings.fix_implicit_replicated_queue_attrs,
            self._table_filter,
        )
        self._scenario: ScenarioBase | None = None

    def add_desired_node(self, node_settings: Types.Attributes | Node):
        self._desired_state_builder.add_node(node_settings)

    def add_desired_table(self, table_settings: Types.Attributes | Table):
        self._desired_state_builder.add_table(table_settings)

    def add_desired_consumer(self, consumer_settings: Types.Attributes | Consumer):
        self._desired_state_builder.add_consumer(consumer_settings)

    def add_desired_producer(self, producer_settings: Types.Attributes | Producer):
        self._desired_state_builder.add_producer(producer_settings)

    def add_desired_pipeline(self, pipeline_settings: Types.Attributes | Pipeline):
        self._desired_state_builder.add_pipeline(pipeline_settings)

    def sync(self, **kwargs) -> bool:
        self._settings.is_ok()  # check settings is ok
        LOG.info(
            "YT Sync started, scenario=%s, dry=%s, scenario params=%s",
            self._scenario_name,
            self._yt_client_factory.is_dry_run(),
            kwargs,
        )
        try:
            self._desired_state_builder.finalize(self._settings)
            self._load_actual_state()
            with self._lock.lock():
                return self._run_scenario(**kwargs)
        finally:
            LOG.info("YT Sync finished")

    def _load_actual_state(self):
        LOG.info("Load actual DB state")
        self.actual = load_actual_db(
            self._settings,
            self._yt_client_factory,
            self.desired,
            self._table_filter,
            self._scenario_name == DumpSpecScenario.SCENARIO_NAME,
        )

    def _run_scenario(self, **kwargs) -> bool:
        scenario_type = get_scenario_type(self._scenario_name)
        self._scenario = scenario_type(self.desired, self.actual, self._settings, self._yt_client_factory)
        self._scenario.setup(**kwargs)
        LOG.info("Run YT Sync scenario %s", self._scenario_name)
        return self._scenario.run()

    @staticmethod
    def disable_normalized_expression():
        # Disables normalized_expression in schema comparision.
        # Should be tested with True for all tables, then removed completely with normalized expression.
        # See YTADMINREQ-43528 for details.
        YtColumn.DISABLE_NORMALIZED_EXPRESSION = True
