import os
import re
from unittest.mock import Mock

import pytest

import yatest.common
import yt.wrapper

from yt.common import wait

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from .clickhouse_cluster import ClickHouseCluster
from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")

PARTITION_COUNT = 4
PARTITION_MESSAGE_COUNT = 50
EXPECTED_COUNT = PARTITION_COUNT * PARTITION_MESSAGE_COUNT

DATABASE = "default"
TABLE = "flow_sink"

# Loopback address the recipe's ClickHouse never binds, so connecting to it on the shared
# native port is refused rather than timing out.
DEAD_HOST = "127.0.0.2"

##################################################################


@pytest.mark.authors(["blinkov"])
def test_clickhouse_cluster_releases_ports_on_cleanup_failure():
    cluster = object.__new__(ClickHouseCluster)
    cluster.processes = {"a1": Mock()}
    cluster.clients = {}
    cluster.logs = {}
    cluster.ports = Mock()
    cluster.stop = Mock(side_effect=RuntimeError("stop failed"))

    with pytest.raises(RuntimeError, match="stop failed"):
        cluster.close()

    cluster.ports.release.assert_called_once_with()


class TestClickHouseSink(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")
    DRIVER_BACKEND = "rpc"

    def setup_method(self, method):
        super(TestClickHouseSink, self).setup_method(method)
        self.ch_port = int(os.environ["RECIPE_CLICKHOUSE_NATIVE_PORT"])
        self.cluster = ClickHouseCluster(self.ch_port)
        self.cluster.start()
        self.ch_host = self.cluster.hosts["a1"]
        self.ch = self.cluster.clients["a1"]

    def teardown_method(self, method):
        try:
            self.cluster.close()
        finally:
            super(TestClickHouseSink, self).teardown_method(method)

    def two_shard_hosts(self):
        return {"a": [self.cluster.hosts["a1"]], "b": [self.cluster.hosts["b1"]]}

    def shard_parameters(self, sharded):
        if not sharded:
            return {}
        return {
            "shard_hosts": self.two_shard_hosts(),
            "sharding_key_columns": ["id"],
        }

    def keeper_path(self, name, table=TABLE):
        suffix = re.sub(r"[^A-Za-z0-9_]", "_", self.test_name)
        return f"/clickhouse/tables/{suffix}/{name[0]}/{table}"

    def recreate_target_table(self, name, keeper_path=None):
        client = self.cluster.clients[name]
        client.execute(f"DROP TABLE IF EXISTS {DATABASE}.{TABLE} SYNC")
        client.execute(f"""
            CREATE TABLE {DATABASE}.{TABLE} (
                id String,
                seq Int64,
                flag Bool,
                category LowCardinality(String),
                code FixedString(4),
                note Nullable(String),
                source String DEFAULT 'flow',
                parity UInt8 MATERIALIZED seq % 2
            )
            ENGINE = ReplicatedMergeTree('{keeper_path or self.keeper_path(name)}', '{name}')
            ORDER BY id
            SETTINGS replicated_deduplication_window = 100000,
                     replicated_deduplication_window_seconds = 604800
            """)

    def create_target_table(self):
        for name in self.cluster.hosts:
            self.recreate_target_table(name)

    TRUTH_TABLE = "flow_truth"

    def create_constrained_target_table(self):
        for name, client in self.cluster.clients.items():
            client.execute(f"DROP TABLE IF EXISTS {DATABASE}.{TABLE} SYNC")
            client.execute(f"""
                CREATE TABLE {DATABASE}.{TABLE} (
                    id String,
                    seq Int64,
                    flag Bool,
                    category LowCardinality(String),
                    code FixedString(4),
                    note Nullable(String),
                    CONSTRAINT even_only CHECK flag = false
                )
                ENGINE = ReplicatedMergeTree('{self.keeper_path(name)}', '{name}')
                ORDER BY id
                SETTINGS replicated_deduplication_window = 100000,
                         replicated_deduplication_window_seconds = 604800
                """)

    def create_plain_target_table(self):
        for client in self.cluster.clients.values():
            client.execute(f"DROP TABLE IF EXISTS {DATABASE}.{TABLE} SYNC")
            client.execute(f"""
                CREATE TABLE {DATABASE}.{TABLE} (
                    id String,
                    seq Int64,
                    flag Bool,
                    category LowCardinality(String),
                    code FixedString(4),
                    note Nullable(String)
                )
                ENGINE = MergeTree()
                ORDER BY id
                """)

    def create_truth_table(self):
        for name, client in self.cluster.clients.items():
            client.execute(f"DROP TABLE IF EXISTS {DATABASE}.{self.TRUTH_TABLE} SYNC")
            client.execute(f"""
                CREATE TABLE {DATABASE}.{self.TRUTH_TABLE} (
                    id String,
                    seq Int64,
                    flag Bool,
                    category LowCardinality(String),
                    code FixedString(4),
                    note Nullable(String)
                )
                ENGINE = ReplicatedMergeTree('{self.keeper_path(name, self.TRUTH_TABLE)}', '{name}')
                ORDER BY id
                SETTINGS replicated_deduplication_window = 100000,
                         replicated_deduplication_window_seconds = 604800
                """)

    def read_back_ids(self):
        result = []
        for name in ("a1", "b1"):
            rows = self.cluster.clients[name].execute(f"SELECT id FROM {DATABASE}.{TABLE}")
            result.extend(row[0] for row in rows)
        return result

    def verify_auto_derived_columns(self):
        rows = []
        for name in ("a1", "b1"):
            rows.extend(
                self.cluster.clients[name].execute(
                    f"SELECT flag, category, length(code), note, source FROM {DATABASE}.{TABLE}"
                )
            )
        assert rows
        for flag, category, code_length, note, source in rows:
            assert source == "flow"
            assert category == ("odd" if flag else "even")
            assert code_length == 4
            assert (note is not None) == bool(flag)

    def prepare_pipeline_config(
        self,
        sink_class_name,
        extra_sinks=None,
        sink_dynamic_parameters=None,
        partition_count=PARTITION_COUNT,
        partition_message_count=PARTITION_MESSAGE_COUNT,
        message_count_mean=None,
        hosts=None,
        shard_hosts=None,
        sharding_key_columns=None,
        config_name="pipeline.yson",
    ):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        computation = pipeline_config["spec"]["computations"]["reader"]
        sink = computation["sinks"]["clickhouse"]
        sink["sink_class_name"] = sink_class_name
        for host_form in ("host", "hosts", "shard_hosts", "sharding_key_columns"):
            sink["parameters"].pop(host_form, None)
        sink["parameters"].update(
            {
                "port": self.ch_port,
                "database": DATABASE,
                "table": TABLE,
            }
        )
        if shard_hosts is not None:
            sink["parameters"]["shard_hosts"] = shard_hosts
            if sharding_key_columns is not None:
                sink["parameters"]["sharding_key_columns"] = sharding_key_columns
        elif hosts is not None:
            sink["parameters"]["hosts"] = hosts
        else:
            sink["parameters"]["host"] = self.ch_host
        if sink_class_name == "NYT::NFlow::TAtMostOnceClickHouseSink":
            sink["parameters"]["at_most_once_strategy"] = {"enabled": True}
        for name, (extra_class_name, extra_table) in (extra_sinks or {}).items():
            computation["sinks"][name] = {
                "sink_class_name": extra_class_name,
                "input_stream_ids": ["rows"],
                "parameters": {
                    "host": self.ch_host,
                    "port": self.ch_port,
                    "database": DATABASE,
                    "table": extra_table,
                },
            }
        sink_dynamic_parameters = dict(sink_dynamic_parameters or {})
        if sink_class_name == "NYT::NFlow::TAtMostOnceClickHouseSink":
            at_most_once_dynamic_parameters = dict(sink_dynamic_parameters.get("at_most_once_strategy", {}))
            # Sanitizers and sandboxing can make serial inserts outlive the 10s product drain default;
            # keep the test drain longer than the 120s delivery wait.
            at_most_once_dynamic_parameters["suspend_destruction_duration"] = "300s"
            sink_dynamic_parameters["at_most_once_strategy"] = at_most_once_dynamic_parameters
        if sink_dynamic_parameters:
            dynamic_computation = pipeline_config["dynamic_spec"]["computations"]["reader"]
            dynamic_computation.setdefault("sinks", {})["clickhouse"] = {"parameters": sink_dynamic_parameters}

        random_source = pipeline_config["dynamic_spec"]["computations"]["reader"]["source_streams"]["random"]
        random_source["parameters"]["partition_count"] = partition_count
        if partition_message_count is None:
            random_source["parameters"].pop("partition_message_count", None)
        else:
            random_source["parameters"]["partition_message_count"] = partition_message_count
        if message_count_mean is not None:
            random_source["parameters"]["message_count_mean"] = message_count_mean

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, config_name)

    def run_pipeline(self, sink_class_name, problems, wait_for_delivery=None, **sink_kwargs):
        run_yt_sync("primary", self.work_yt_path)
        self.create_target_table()
        pipeline_config_path = self.prepare_pipeline_config(sink_class_name, **sink_kwargs)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=4 if problems else 1,
            controllers_count=2 if problems else 1,
            problems=problems,
        ):
            self.wait_pipeline_state("completed", timeout=300)
            if wait_for_delivery:
                wait(wait_for_delivery, timeout=120)

    def assert_initialization_rejected(self, pipeline_config_path):
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            self.wait_for_pipeline_error("Failed to initialize ClickHouse writer", timeout=120)
            assert self.client.get_pipeline_state(self.pipeline_path) != "completed"

    def assert_no_rows(self, names):
        for name in names:
            assert self.cluster.clients[name].execute(f"SELECT count() FROM {DATABASE}.{TABLE}") == [(0,)]

    @pytest.mark.authors(["blinkov"])
    @pytest.mark.parametrize("problems", [False, True], ids=["stable", "unstable"])
    def test_exactly_once(self, problems):
        self.run_pipeline("NYT::NFlow::TClickHouseBatchingSink", problems)
        ids = self.read_back_ids()
        assert len(ids) == EXPECTED_COUNT
        assert len(set(ids)) == EXPECTED_COUNT
        self.verify_auto_derived_columns()

    @pytest.mark.authors(["blinkov"])
    @pytest.mark.parametrize("sharded", [False, True], ids=["single_shard", "two_shards"])
    def test_at_least_once(self, sharded):
        self.run_pipeline(
            "NYT::NFlow::TAtLeastOnceClickHouseSink",
            problems=True,
            **self.shard_parameters(sharded),
        )
        ids = self.read_back_ids()
        assert len(set(ids)) == EXPECTED_COUNT

    @pytest.mark.authors(["blinkov"])
    @pytest.mark.parametrize("flow_problems", [False, True], ids=["stable", "unstable"])
    @pytest.mark.parametrize("sharded", [False, True], ids=["single_shard", "two_shards"])
    def test_at_most_once(self, sharded, flow_problems):
        wait_for_delivery = None if flow_problems else lambda: len(set(self.read_back_ids())) == EXPECTED_COUNT
        self.run_pipeline(
            "NYT::NFlow::TAtMostOnceClickHouseSink",
            problems=flow_problems,
            wait_for_delivery=wait_for_delivery,
            **self.shard_parameters(sharded),
        )
        ids = self.read_back_ids()
        assert len(ids) == len(set(ids))
        if flow_problems:
            assert 0 < len(ids) <= EXPECTED_COUNT
        else:
            assert len(ids) == EXPECTED_COUNT

    @pytest.mark.authors(["blinkov"])
    @pytest.mark.parametrize("problems", [False, True], ids=["stable", "unstable"])
    def test_exactly_once_two_shards(self, problems):
        self.run_pipeline(
            "NYT::NFlow::TShardedClickHouseBatchingSink",
            problems,
            shard_hosts=self.two_shard_hosts(),
            sharding_key_columns=["id"],
        )
        ids = self.read_back_ids()
        assert len(ids) == EXPECTED_COUNT
        assert len(set(ids)) == EXPECTED_COUNT
        self.verify_auto_derived_columns()

    @pytest.mark.authors(["blinkov"])
    def test_dead_configured_endpoint_rejects_startup(self):
        run_yt_sync("primary", self.work_yt_path)
        self.create_target_table()
        pipeline_config_path = self.prepare_pipeline_config(
            "NYT::NFlow::TClickHouseBatchingSink",
            hosts=[DEAD_HOST, self.cluster.hosts["a1"]],
            sink_dynamic_parameters={"retry_backoff": 100},
        )
        self.assert_initialization_rejected(pipeline_config_path)
        self.assert_no_rows(("a1", "b1"))

    @pytest.mark.authors(["blinkov"])
    def test_failover_accepts_same_replicated_table(self):
        self.create_target_table()
        identities = []
        for name in ("a1", "a2"):
            identities.append(
                self.cluster.clients[name].execute(
                    "SELECT zookeeper_name, zookeeper_path, replica_name "
                    f"FROM system.replicas WHERE database = '{DATABASE}' AND table = '{TABLE}'"
                )[0]
            )
        assert identities[0][:2] == identities[1][:2]
        assert identities[0][2] != identities[1][2]
        self.run_pipeline(
            "NYT::NFlow::TClickHouseBatchingSink",
            problems=False,
            hosts=[self.cluster.hosts["a1"], self.cluster.hosts["a2"]],
        )
        ids = self.read_back_ids()
        assert len(ids) == EXPECTED_COUNT
        assert len(set(ids)) == EXPECTED_COUNT
        self.verify_auto_derived_columns()

    @pytest.mark.authors(["blinkov"])
    def test_live_session_failover_without_client_recreation(self):
        run_yt_sync("primary", self.work_yt_path)
        self.create_target_table()
        pipeline_config_path = self.prepare_pipeline_config(
            "NYT::NFlow::TClickHouseBatchingSink",
            hosts=[self.cluster.hosts["a1"], self.cluster.hosts["a2"]],
            partition_message_count=None,
            config_name="pipeline_live_failover.yson",
        )
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            replica = self.cluster.clients["a2"]
            wait(
                lambda: replica.execute(f"SELECT count() FROM {DATABASE}.{TABLE}")[0][0] > 0,
                timeout=120,
            )
            before = replica.execute(f"SELECT count() FROM {DATABASE}.{TABLE}")[0][0]
            self.cluster.stop("a1")
            wait(
                lambda: replica.execute(f"SELECT count() FROM {DATABASE}.{TABLE}")[0][0] > before,
                timeout=120,
            )
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=300)
            ids = [row[0] for row in replica.execute(f"SELECT id FROM {DATABASE}.{TABLE}")]
        assert len(ids) == len(set(ids))

    @pytest.mark.authors(["blinkov"])
    def test_sharded_live_failover_preserves_independent_shard(self):
        run_yt_sync("primary", self.work_yt_path)
        self.create_target_table()
        pipeline_config_path = self.prepare_pipeline_config(
            "NYT::NFlow::TShardedClickHouseBatchingSink",
            shard_hosts={
                "a": [self.cluster.hosts["a1"], self.cluster.hosts["a2"]],
                "b": [self.cluster.hosts["b1"]],
            },
            sharding_key_columns=["id"],
            partition_message_count=None,
            config_name="pipeline_sharded_live_failover.yson",
        )
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            replica = self.cluster.clients["a2"]
            independent_shard = self.cluster.clients["b1"]
            wait(
                lambda: replica.execute(f"SELECT count() FROM {DATABASE}.{TABLE}")[0][0] > 0
                and independent_shard.execute(f"SELECT count() FROM {DATABASE}.{TABLE}")[0][0] > 0,
                timeout=120,
            )
            replica_before = replica.execute(f"SELECT count() FROM {DATABASE}.{TABLE}")[0][0]
            independent_before = independent_shard.execute(f"SELECT count() FROM {DATABASE}.{TABLE}")[0][0]
            self.cluster.stop("a1")
            wait(
                lambda: replica.execute(f"SELECT count() FROM {DATABASE}.{TABLE}")[0][0] > replica_before
                and independent_shard.execute(f"SELECT count() FROM {DATABASE}.{TABLE}")[0][0] > independent_before,
                timeout=120,
            )
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=300)
            ids = [row[0] for row in replica.execute(f"SELECT id FROM {DATABASE}.{TABLE}")]
            ids.extend(row[0] for row in independent_shard.execute(f"SELECT id FROM {DATABASE}.{TABLE}"))
        assert len(ids) == len(set(ids))

    @pytest.mark.authors(["blinkov"])
    def test_failover_rejects_distinct_keeper_path(self):
        run_yt_sync("primary", self.work_yt_path)
        self.create_target_table()
        suffix = re.sub(r"[^A-Za-z0-9_]", "_", self.test_name)
        self.recreate_target_table("a2", f"/clickhouse/tables/{suffix}/distinct/{TABLE}")
        pipeline_config_path = self.prepare_pipeline_config(
            "NYT::NFlow::TClickHouseBatchingSink",
            hosts=[self.cluster.hosts["a1"], self.cluster.hosts["a2"]],
            sink_dynamic_parameters={"retry_backoff": 100},
        )
        self.assert_initialization_rejected(pipeline_config_path)
        self.assert_no_rows(("a1", "a2"))

    @pytest.mark.authors(["blinkov"])
    @pytest.mark.parametrize(
        "expression_kind",
        ["default", "materialized", "alias"],
    )
    def test_shards_reject_different_schema_expression(self, expression_kind):
        run_yt_sync("primary", self.work_yt_path)
        self.create_target_table()
        if expression_kind == "default":
            self.cluster.clients["b1"].execute(
                f"ALTER TABLE {DATABASE}.{TABLE} MODIFY COLUMN source String DEFAULT 'other'"
            )
        elif expression_kind == "materialized":
            self.cluster.clients["b1"].execute(
                f"ALTER TABLE {DATABASE}.{TABLE} " "MODIFY COLUMN parity UInt8 MATERIALIZED (seq + 1) % 2"
            )
        else:
            self.cluster.clients["a1"].execute(f"ALTER TABLE {DATABASE}.{TABLE} ADD COLUMN alias_seq Int64 ALIAS seq")
            self.cluster.clients["b1"].execute(
                f"ALTER TABLE {DATABASE}.{TABLE} ADD COLUMN alias_seq Int64 ALIAS seq + 1"
            )
        pipeline_config_path = self.prepare_pipeline_config(
            "NYT::NFlow::TShardedClickHouseBatchingSink",
            shard_hosts=self.two_shard_hosts(),
            sharding_key_columns=["id"],
            sink_dynamic_parameters={"retry_backoff": 100},
        )
        self.assert_initialization_rejected(pipeline_config_path)
        self.assert_no_rows(("a1", "b1"))

    @pytest.mark.authors(["blinkov"])
    @pytest.mark.parametrize(
        "sink_class_name",
        [
            "NYT::NFlow::TClickHouseBatchingSink",
            "NYT::NFlow::TAtLeastOnceClickHouseSink",
            "NYT::NFlow::TAtMostOnceClickHouseSink",
        ],
        ids=["exactly_once", "at_least_once", "at_most_once"],
    )
    def test_failover_rejects_plain_tables_for_all_guarantees(self, sink_class_name):
        run_yt_sync("primary", self.work_yt_path)
        self.create_plain_target_table()
        pipeline_config_path = self.prepare_pipeline_config(
            sink_class_name,
            hosts=[self.cluster.hosts["a1"], self.cluster.hosts["a2"]],
            sink_dynamic_parameters={"retry_backoff": 100},
        )
        self.assert_initialization_rejected(pipeline_config_path)
        self.assert_no_rows(("a1", "a2"))

    @pytest.mark.authors(["blinkov"])
    def test_endpoint_missing_table_is_rejected(self):
        run_yt_sync("primary", self.work_yt_path)
        self.create_target_table()
        self.cluster.clients["a2"].execute(f"DROP TABLE {DATABASE}.{TABLE} SYNC")
        pipeline_config_path = self.prepare_pipeline_config(
            "NYT::NFlow::TClickHouseBatchingSink",
            hosts=[self.cluster.hosts["a1"], self.cluster.hosts["a2"]],
            sink_dynamic_parameters={"retry_backoff": 100},
        )
        self.assert_initialization_rejected(pipeline_config_path)
        self.assert_no_rows(("a1",))

    def read_sink_states(self):
        return self.client.select_rows(
            f"* from [{self.pipeline_path}/states]",
            format=yt.wrapper.format.YsonFormat(encoding=None),
        )

    def undelivered_batch_bounds(self):
        return [
            (row[b"key"], row[b"state"][b"batch_bounds"])
            for row in self.read_sink_states()
            if row[b"name"] == b"/sinks/clickhouse/v0" and row[b"state"].get(b"batch_bounds")
        ]

    def persisted_topology_fingerprints(self):
        return {
            row[b"state"][b"topology_fingerprint"]
            for row in self.read_sink_states()
            if row[b"name"] == b"/sinks/clickhouse/shard_topology/v0"
        }

    @pytest.mark.authors(["blinkov"])
    def test_topology_change_with_undelivered_batches_is_refused(self):
        run_yt_sync("primary", self.work_yt_path)
        self.create_constrained_target_table()
        undrained_dynamic_parameters = {"retry_backoff": 100, "max_insert_attempts": 3}
        unsharded_config = self.prepare_pipeline_config(
            "NYT::NFlow::TClickHouseBatchingSink",
            sink_dynamic_parameters=undrained_dynamic_parameters,
            config_name="pipeline_undrained_unsharded.yson",
        )
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": unsharded_config},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            self.wait_for_pipeline_error("Giving up insert into ClickHouse", timeout=120)
            wait(lambda: bool(self.undelivered_batch_bounds()), timeout=120)
            assert self.persisted_topology_fingerprints() == {b"unsharded"}

        sharded_config = self.prepare_pipeline_config(
            "NYT::NFlow::TShardedClickHouseBatchingSink",
            sink_dynamic_parameters=undrained_dynamic_parameters,
            shard_hosts=self.two_shard_hosts(),
            sharding_key_columns=["id"],
            config_name="pipeline_undrained_sharded.yson",
        )
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": sharded_config},
            workers_count=1,
            controllers_count=1,
            problems=False,
            # A graceful update stops the pipeline by draining it, and a pipeline holding a batch
            # that its target rejects can never drain: the runner would wait for Stopped forever
            # and never apply the new spec. Pausing is what actually delivers a topology change to
            # an undrained sink, which is the case the guard exists for.
            additional_env={"YT_FLOW_GRACEFUL_UPDATE": "0"},
        ):
            self.wait_for_pipeline_error("Refusing to start the ClickHouse sink", timeout=120)
            assert self.client.get_pipeline_state(self.pipeline_path) != "completed"
            assert self.undelivered_batch_bounds()
            assert self.persisted_topology_fingerprints() == {b"unsharded"}
            ids = self.read_back_ids()
            assert len(ids) == len(set(ids))

    @pytest.mark.authors(["blinkov"])
    def test_resharding_after_drain_keeps_rows_intact(self):
        run_yt_sync("primary", self.work_yt_path)
        self.create_target_table()
        minimum_rows_before_resharding = EXPECTED_COUNT
        unsharded_config = self.prepare_pipeline_config(
            "NYT::NFlow::TClickHouseBatchingSink",
            partition_message_count=None,
            config_name="pipeline_unsharded.yson",
        )
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": unsharded_config},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            wait(lambda: len(self.read_back_ids()) >= minimum_rows_before_resharding, timeout=300)
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=300)
            assert not self.undelivered_batch_bounds()
            assert self.persisted_topology_fingerprints() == {b"unsharded"}
            drained_ids = self.read_back_ids()

        sharded_config = self.prepare_pipeline_config(
            "NYT::NFlow::TShardedClickHouseBatchingSink",
            partition_message_count=None,
            shard_hosts=self.two_shard_hosts(),
            sharding_key_columns=["id"],
            config_name="pipeline_sharded.yson",
        )

        def sharded_topology_stamped():
            fingerprints = self.persisted_topology_fingerprints()
            return bool(fingerprints) and b"unsharded" not in fingerprints

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": sharded_config},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            wait(sharded_topology_stamped, timeout=300)
            wait(lambda: len(self.read_back_ids()) > len(drained_ids), timeout=300)
            sharded_ids = self.read_back_ids()

        assert set(drained_ids).issubset(set(sharded_ids))
        assert len(sharded_ids) == len(set(sharded_ids))

    @pytest.mark.authors(["blinkov"])
    def test_distinct_shard_tables_preserve_delivery_not_historical_colocation(self):
        run_yt_sync("primary", self.work_yt_path)
        self.create_target_table()
        first_config = self.prepare_pipeline_config(
            "NYT::NFlow::TShardedClickHouseBatchingSink",
            partition_message_count=None,
            shard_hosts={"a": [self.cluster.hosts["a1"]]},
            sharding_key_columns=["category"],
            config_name="pipeline_physical_shard_a.yson",
        )
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": first_config},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            wait(
                lambda: self.cluster.clients["a1"].execute(f"SELECT count() FROM {DATABASE}.{TABLE}")[0][0]
                >= EXPECTED_COUNT,
                timeout=300,
            )
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=300)
            assert not self.undelivered_batch_bounds()
            drained_ids = self.read_back_ids()
            old_categories = {
                row[0]
                for row in self.cluster.clients["a1"].execute(f"SELECT DISTINCT category FROM {DATABASE}.{TABLE}")
            }

        second_config = self.prepare_pipeline_config(
            "NYT::NFlow::TShardedClickHouseBatchingSink",
            partition_message_count=None,
            shard_hosts={"b": [self.cluster.hosts["b1"]]},
            sharding_key_columns=["category"],
            config_name="pipeline_physical_shard_b.yson",
        )
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": second_config},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            wait(
                lambda: self.cluster.clients["b1"].execute(f"SELECT count() FROM {DATABASE}.{TABLE}")[0][0] > 0,
                timeout=300,
            )
            new_categories = {
                row[0]
                for row in self.cluster.clients["b1"].execute(f"SELECT DISTINCT category FROM {DATABASE}.{TABLE}")
            }

        all_ids = self.read_back_ids()
        assert old_categories & new_categories
        assert set(drained_ids).issubset(set(all_ids))
        assert len(all_ids) == len(set(all_ids))

    @pytest.mark.authors(["blinkov"])
    def test_at_most_once_initialization_failure_stalls_source(self):
        run_yt_sync("primary", self.work_yt_path)
        self.ch.execute(f"DROP TABLE IF EXISTS {DATABASE}.{TABLE} SYNC")
        pipeline_config_path = self.prepare_pipeline_config(
            "NYT::NFlow::TAtMostOnceClickHouseSink",
            sink_dynamic_parameters={"retry_backoff": 100},
            partition_count=1,
            partition_message_count=1,
            message_count_mean=0,
        )

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):

            def read_source_states():
                rows = self.client.select_rows(
                    f"* from [{self.pipeline_path}/states]",
                    format=yt.wrapper.format.YsonFormat(encoding=None),
                )
                return [
                    row[b"state"]
                    for row in rows
                    if row[b"computation_id"] == b"reader" and row[b"name"] == b"/$active_source/v0"
                ]

            wait(lambda: bool(read_source_states()), timeout=120)
            dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)["spec"]
            dynamic_spec["computations"]["reader"]["source_streams"]["random"]["parameters"][
                "message_count_mean"
            ] = 1_000_000
            self.client.set_pipeline_dynamic_spec(self.pipeline_path, dynamic_spec)

            self.wait_for_pipeline_error("Failed to initialize ClickHouse writer", timeout=120)
            assert self.client.get_pipeline_state(self.pipeline_path) != "completed"

            source_states = read_source_states()
            assert source_states
            source_offsets = []
            for state in source_states:
                assert b"persisted_offset_exclusive_v2" in state
                offset = state[b"persisted_offset_exclusive_v2"]
                source_offsets.append(0 if not offset else int(offset[0]))
            assert sum(source_offsets) == 0

            self.create_target_table()
            self.wait_pipeline_state("completed", timeout=300)
            wait(lambda: len(set(self.read_back_ids())) == 1, timeout=120)

        ids = self.read_back_ids()
        assert len(ids) == 1
        assert len(set(ids)) == 1

    @pytest.mark.authors(["blinkov"])
    def test_at_most_once_uninsertable(self):
        run_yt_sync("primary", self.work_yt_path)
        self.create_constrained_target_table()
        self.create_truth_table()
        pipeline_config_path = self.prepare_pipeline_config(
            "NYT::NFlow::TAtMostOnceClickHouseSink",
            extra_sinks={"truth": ("NYT::NFlow::TClickHouseBatchingSink", self.TRUTH_TABLE)},
        )

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            self.wait_pipeline_state("completed", timeout=300)

            truth = self.ch.execute(f"SELECT id, flag FROM {DATABASE}.{self.TRUTH_TABLE}")
            assert len(truth) == EXPECTED_COUNT
            assert len({row[0] for row in truth}) == EXPECTED_COUNT
            expected_ids = {row[0] for row in truth if not row[1]}
            assert expected_ids

            def all_valid_rows_arrived():
                ids = self.read_back_ids()
                return len(ids) == len(expected_ids) and set(ids) == expected_ids

            wait(all_valid_rows_arrived, timeout=120)

        sink_ids = self.read_back_ids()
        assert len(sink_ids) == len(set(sink_ids))
        assert set(sink_ids) == expected_ids

    @pytest.mark.authors(["blinkov"])
    @pytest.mark.parametrize(
        "sink_class_name",
        ["NYT::NFlow::TClickHouseBatchingSink", "NYT::NFlow::TAtLeastOnceClickHouseSink"],
        ids=["exactly_once", "at_least_once"],
    )
    def test_uninsertable_fails_loudly(self, sink_class_name):
        run_yt_sync("primary", self.work_yt_path)
        self.create_constrained_target_table()
        pipeline_config_path = self.prepare_pipeline_config(
            sink_class_name,
            sink_dynamic_parameters={"retry_backoff": 100, "max_insert_attempts": 3},
        )

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            self.wait_for_pipeline_error("Giving up insert into ClickHouse", timeout=120)
            assert self.client.get_pipeline_state(self.pipeline_path) != "completed"

        rows = self.ch.execute(f"SELECT id, flag FROM {DATABASE}.{TABLE}")
        assert all(not flag for _, flag in rows)
        ids = [row[0] for row in rows]
        assert len(ids) == len(set(ids))
