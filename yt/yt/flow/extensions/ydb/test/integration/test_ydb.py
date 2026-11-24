import copy
import os
import time
import xml.etree.ElementTree as ElementTree
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import requests
import yatest.common
import ydb

from library.python.port_manager import PortManager

from yt.common import wait
from yt.wrapper import yson

from yt.yt.flow.extensions.ydb.python import read_all_from_topic
from yt.yt.flow.extensions.ydb.python.codecs import ENCODERS
from yt.yt.flow.extensions.ydb.test.proto.message_pb2 import TTestMessage
from yt.yt.flow.library.python.bullied_process import ProblemsConfig
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync
from yt.yt.flow.library.python.queue import batching_write_rows

from public_fixture.federation_discovery_mock import FederationDiscoveryMock
from public_fixture.lenval import pack_lenval, split_lenval
from public_fixture.public_ydb import LOGIN_USER, PublicYdbInstance, load_lock, locate_fixture_binaries, render_config
from public_fixture.ydb_outage_proxy import YdbOutageProxy

PIPELINE_DIRECTORY = "yt/yt/flow/extensions/ydb/test/pipeline"
PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{PIPELINE_DIRECTORY}/pipeline.yson")
DELIVERY_PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{PIPELINE_DIRECTORY}/delivery_pipeline.yson")
PROBE_PATH = "yt/yt/flow/extensions/ydb/test/probe/ydb_fixture_probe"
SOURCE_GTEST_PATH = "yt/yt/flow/extensions/ydb/cpp/unittests/unittester-flow-ydb"
SOURCE_GTEST_TIMEOUT_SECONDS = 300
DELIVERY_TEST_RUN_ID = "ydb-delivery-validation-v1"
DELIVERY_LANES = ("alo", "alo_second", "exactly_once")
DELIVERY_SINK_CLASSES = {
    "alo": "NYT::NFlow::TYdbSink",
    "alo_second": "NYT::NFlow::TYdbSink",
    "exactly_once": "NYT::NFlow::TYdbTwoPhaseCommitSink",
}

if yatest.common.context.sanitize is not None:
    EVENT_COUNT = 100
else:
    EVENT_COUNT = 1000

EVENT_WATERMARK = 1700000000
FLOW_LOGIN_USER = "flowlogin"
FLOW_LOGIN_PASSWORD = "YtflowSyntheticFlowLogin2026"
FLOW_LOGIN_PASSWORD_ENV_VAR = "FLOW_YDB_LOGIN_PASSWORD"
FLOW_QUEUE_META_NAME = "_ytflow_queue_meta"
LEGACY_FLOW_QUEUE_META_NAME = "flow_queue_meta"
KAFKA_KEY_ATTRIBUTE_NAME = "__key"
PIPELINE_ATTRIBUTE = b"ydb-test"
DEFERRED_PUBLICATION_STATE_NAME = "/sinks/sink/deferred_publication/v0"
SUPPORTED_CODECS = [ydb.TopicCodec.RAW, ydb.TopicCodec.GZIP, ydb.TopicCodec.ZSTD]

DEFERRED_PUBLISH_TESTS = {
    "test_delivery_validation_full_graph_exactly_once",
    "test_delivery_validation_pending_publish_holds_watermark",
    "test_delivery_validation_rejects_late_exactly_once_duplicate",
    "test_deferred_publication_cancel_reuses_sequence",
    "test_deferred_publication_selects_writable_cluster",
    "test_deferred_publications",
    "test_ordered_source_deferred_publications",
}
SINGLE_PARTITION_TESTS = {
    "test_deferred_publications",
    "test_deferred_publication_cancel_reuses_sequence",
    "test_deferred_publication_switches_cluster_between_waves",
    "test_ordered_source_deferred_publications",
}

ORIGINAL_TEST_VARIANTS = {
    "test_delivery_validation_full_graph_alo": [{"stable"}, {"chaos"}],
    "test_delivery_validation_full_graph_exactly_once": [{"stable"}, {"chaos"}],
    "test_delivery_validation_pending_publish_holds_watermark": [set()],
    "test_delivery_validation_subgraphs_are_independent": [set()],
    "test_delivery_validation_source_outage_holds_watermark": [set()],
    "test_delivery_validation_rejects_late_exactly_once_duplicate": [set()],
    "test_delivery_validation_finite_closes_last_window_and_cleans_state": [set()],
    "test_delivery_validation_finite_reports_totally_missing_probe": [set()],
    "test_deferred_publication_cancel_reuses_sequence": [set()],
    "test_deferred_publications": [
        {"plain", "regular"},
        {"plain", "chaos"},
        {"framing", "regular"},
        {"framing", "chaos"},
    ],
    "test_ordered_source_deferred_publications": [{"swift"}, {"transform"}],
    "test_deferred_publication_selects_writable_cluster": [set()],
    "test_deferred_publication_switches_cluster_between_waves": [{"publish"}, {"write_ack"}],
    "test_hard_commit_preserves_read_session": [set()],
    "test_basic": [
        {"1c_1w_stable"},
        {"2c_4w_unstable"},
        {"1c_1w_not_zero_start_offset"},
        {"2c_4w_unstable_at_least_once"},
    ],
    "test_flow_queue_meta": [{FLOW_QUEUE_META_NAME}, {LEGACY_FLOW_QUEUE_META_NAME}],
    "test_borrows_partition_count_from_answering_cluster": [set()],
    "test_federation_read_only_cluster": [set()],
    "test_dead_cluster_watermark": [set()],
    "test_sink_attributes": [{"False"}, {"True"}],
    "test_source_attributes": [set()],
    "test_partition_change_and_cache": [set()],
    "test_malformed_message": [{"fail"}, {"drop"}, {"keep"}],
}


def pack_proto_framing(records, batch_size=10):
    return [
        pack_lenval(record.SerializeToString() for record in records[i : i + batch_size])
        for i in range(0, len(records), batch_size)
    ]


def unpack_proto_framing(records):
    return [TTestMessage.FromString(payload) for record in records for payload in split_lenval(record)]


def get_message_key(message):
    assert message.HasField("Key")
    return message.Key


def get_message_row(message):
    assert message.HasField("Data")
    return {"Key": get_message_key(message), "Data": message.Data}


def generate_log(count=EVENT_COUNT):
    return [{"Key": str(i), "Data": f"data_{i}"} for i in range(count)]


def is_pure_heartbeat(message):
    raw_meta = message.metadata_items.get(FLOW_QUEUE_META_NAME)
    return raw_meta is not None and yson.loads(raw_meta).get("pure_heartbeat", False)


def parametrize_ids(mark):
    argvalues = mark.args[1]
    explicit_ids = mark.kwargs.get("ids")
    if explicit_ids is not None:
        return list(explicit_ids)
    return [getattr(value, "id", None) or str(value) for value in argvalues]


def collect_variants(function):
    variants = [frozenset()]
    for mark in getattr(function, "pytestmark", []):
        if mark.name == "parametrize":
            variants = [variant | {test_id} for variant in variants for test_id in parametrize_ids(mark)]
    return variants


@pytest.mark.authors(["pechatnov"])
def test_registered_source_behavior_against_real_server():
    expected_cases = {
        "DeliveredRecordCarriesEverySourceField",
        "InactivePartitionCompletesOnlyAfterItsReadMessagesPersist",
        "KeptMalformedPayloadUsesTheUnparsedPayloadSchema",
        "OnlyAChangedRestartTokenReplacesTheReadSession",
        "QueuedReconfigurationsApplyTheirOwnOptions",
        "ReadBlockedPartitionResumesFromItsPersistedOffset",
        "RestoredOffsetReachesTheSessionBeforeItStarts",
        "SplitPartitionCompletesOnlyAfterItsReadMessagesAreCommitted",
        "StableReplayRepeatsReadGroupsUnderTheirRememberedPolicy",
        "SuccessfulDiscoveryKeepsLatchedReadOnlyInstant",
    }
    result_path = Path(yatest.common.output_path("live-source-gtest.xml"))
    with PortManager() as port_manager, ExitStack() as stack:
        instance = PublicYdbFixtures(port_manager, "live-source").start(stack, "main")
        environment = {name: value for name, value in os.environ.items() if not name.startswith("YDB")}
        environment.update(
            {
                "YDB_TOKEN": root_token(instance),
                "YTFLOW_PUBLIC_YDB_ENDPOINT": instance.endpoint,
                "YTFLOW_PUBLIC_YDB_DATABASE": instance.database,
            }
        )
        yatest.common.execute(
            [
                yatest.common.binary_path(SOURCE_GTEST_PATH),
                "--gtest_filter=TYdbLiveSourceTest.*",
                f"--gtest_output=xml:{result_path}",
            ],
            env=environment,
            timeout=SOURCE_GTEST_TIMEOUT_SECONDS,
            stdout=yatest.common.output_path("live-source-gtest.out"),
            stderr=yatest.common.output_path("live-source-gtest.err"),
        )
    cases = list(ElementTree.parse(result_path).getroot().iter("testcase"))
    assert {case.get("name") for case in cases} == expected_cases
    for case in cases:
        assert case.get("result", "completed") == "completed", case.get("name")
        assert case.find("skipped") is None, case.get("name")
        assert case.find("failure") is None, case.get("name")


@pytest.mark.authors(["pechatnov"])
def test_every_original_integration_variant_has_a_public_destination():
    assert len(ORIGINAL_TEST_VARIANTS) == 23
    for name, expected_variants in ORIGINAL_TEST_VARIANTS.items():
        function = getattr(TestYdbConnector, name)
        actual_variants = collect_variants(function)
        assert len(actual_variants) == len(set(actual_variants)), name
        assert set(actual_variants) == {frozenset(variant) for variant in expected_variants}, name


class PublicYdbFixtures:
    def __init__(self, port_manager, output_name):
        self._port_manager = port_manager
        self._output_name = output_name
        self._lock = load_lock()
        self._binaries = locate_fixture_binaries(self._lock)

    def start(self, stack, name, deferred_publish=False):
        binary, cli = self._binaries
        ports = tuple(tuple(self._port_manager.get_port() for _ in range(3)) for _ in range(2))
        probe_modes = ("deferred-enabled",) if deferred_publish else ("ordinary",)
        instance = PublicYdbInstance(
            Path(yatest.common.output_path(f"{self._output_name}-{name}")),
            binary,
            self._lock,
            render_config(deferred_publish),
            ports,
            yatest.common.binary_path(PROBE_PATH),
            cli,
            probe_modes=probe_modes,
        )
        return stack.enter_context(instance)


def root_token(instance):
    driver_config = ydb.DriverConfig(endpoint=f"grpc://{instance.endpoint}", database=instance.database)
    return ydb.StaticCredentials(driver_config, LOGIN_USER).token


def provision_flow_login(instance):
    settings = ydb.BaseRequestSettings().with_timeout(30)
    session = instance.driver.table_client.session().create(settings=settings)
    try:
        session.execute_scheme(f"CREATE USER {FLOW_LOGIN_USER} PASSWORD '{FLOW_LOGIN_PASSWORD}';", settings)
        session.execute_scheme(f"GRANT ALL ON `{instance.database}` TO {FLOW_LOGIN_USER};", settings)
    finally:
        session.delete(settings=settings)


def create_topic(instance, topic, partition_count, consumers):
    ydb.retry_operation_sync(
        lambda: instance.driver.topic_client.create_topic(
            topic,
            supported_codecs=SUPPORTED_CODECS,
            min_active_partitions=partition_count,
            consumers=consumers,
        )
    )


class TestYdbConnector(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{PIPELINE_DIRECTORY}/pipeline")

    def setup_method(self, method):
        super().setup_method(method)
        self.stack = ExitStack()
        self.fixtures = PublicYdbFixtures(self.port_manager, self.test_name)
        self.partition_count = 1 if method.__name__ in SINGLE_PARTITION_TESTS else 5
        try:
            self.use_instance(self.start_ydb("main", deferred_publish=method.__name__ in DEFERRED_PUBLISH_TESTS))
            self.input_topic = f"{self.test_name}-test-topic"
            self.input_consumer = f"{self.test_name}-test-consumer"
            self.check_consumer = f"{self.test_name}-check-consumer"
            self.output_topic = f"{self.test_name}-test-output-topic"
            self.validation_topic = f"{self.test_name}-validation-topic"
            for topic in (self.input_topic, self.output_topic):
                create_topic(self.ydb, topic, self.partition_count, [self.input_consumer, self.check_consumer])
            create_topic(self.ydb, self.validation_topic, 1, [self.check_consumer])
            run_yt_sync("primary", self.work_yt_path)
        except BaseException:
            self.stack.close()
            raise

    def teardown_method(self, method):
        self.stack.close()
        super().teardown_method(method)

    def start_ydb(self, name, deferred_publish=False):
        return self.fixtures.start(self.stack, name, deferred_publish)

    def use_instance(self, instance):
        self.ydb = instance
        self.driver = instance.driver
        self.ydb_endpoint = instance.endpoint
        self.ydb_database = instance.database
        self.flow_env = {"YDB_TOKEN": root_token(instance)}

    def start_flow(self, config_path, workers_count=1, controllers_count=1, **arguments):
        return self.start_flow_process_federation(
            pipeline_binary_args={"--config": config_path},
            workers_count=workers_count,
            controllers_count=controllers_count,
            additional_env=self.flow_env,
            **arguments,
        )

    def fill_input_topic(self, driver=None, key_prefix="", metadata_items=None, record_count=EVENT_COUNT):
        driver = driver if driver is not None else self.driver
        with driver.topic_client.writer(self.input_topic, codec=ydb.TopicCodec.ZSTD, encoders=ENCODERS) as writer:
            rows = [{"Key": key_prefix + row["Key"], "Data": row["Data"]} for row in generate_log(record_count)]
            packed = pack_proto_framing([TTestMessage(**row) for row in rows])
            if metadata_items is None:
                batching_write_rows(packed, writer.write)
            else:
                batching_write_rows(
                    packed,
                    lambda batch: writer.write(
                        [ydb.TopicWriterMessage(data=data, metadata_items=metadata_items) for data in batch]
                    ),
                )
            writer.flush()

    def write_heartbeats(self, watermark, meta_name=FLOW_QUEUE_META_NAME, driver=None):
        driver = driver if driver is not None else self.driver
        for partition_id in range(self.partition_count):
            with driver.topic_client.writer(
                self.input_topic, partition_id=partition_id, codec=ydb.TopicCodec.ZSTD, encoders=ENCODERS
            ) as writer:
                writer.write(
                    ydb.TopicWriterMessage(
                        data="",
                        metadata_items={meta_name: yson.dumps({"event_watermark": watermark, "pure_heartbeat": True})},
                    )
                )
                writer.flush()

    def get_heartbeat_watermark(self):
        messages = read_all_from_topic(self.driver, self.output_topic, self.check_consumer, detailed=True)
        watermarks = {partition_id: 0 for partition_id in range(self.partition_count)}
        for partition_id, partition_messages in messages.items():
            for message in partition_messages:
                raw_meta = message.metadata_items.get(FLOW_QUEUE_META_NAME)
                if raw_meta is not None:
                    meta = yson.loads(raw_meta)
                    watermarks[partition_id] = max(meta.get("event_watermark", 0), watermarks[partition_id])
        return min(watermarks.values())

    def prepare_pipeline_config(
        self,
        finite=True,
        disable_partitions_cache_updating=False,
        name_suffix="",
        at_least_once=False,
        use_flow_queue_meta=False,
        malformed_message_policy=None,
        source_parameters=None,
        source_dynamic_parameters=None,
        framing=True,
        sink_attributes_column=None,
        source_attributes_column=None,
        try_parse_flow_queue_meta=None,
        flow_queue_meta_name=None,
    ):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)
        if disable_partitions_cache_updating:
            update_partition_count_period_ms = timedelta(days=100).total_seconds() * 1000
        else:
            update_partition_count_period_ms = 1000

        read = pipeline_config["spec"]["computations"]["Read"]
        source = read["source_streams"]["input_topic"]["parameters"]
        source.update(
            {
                "endpoint": self.ydb_endpoint,
                "database": self.ydb_database,
                "topic": self.input_topic,
                "consumer": self.input_consumer,
                "finite": finite,
                "update_partition_count_period": update_partition_count_period_ms,
                **(source_parameters or {}),
            }
        )
        if use_flow_queue_meta:
            read["watermark_strategy"] = {
                "watermark_generator": {"use_source_watermark": True, "out_of_orderness_bound": "1m"}
            }
        if source_attributes_column is not None:
            source["attributes_column"] = source_attributes_column
        if try_parse_flow_queue_meta is not None:
            source["try_parse_flow_queue_meta"] = try_parse_flow_queue_meta
        if flow_queue_meta_name is not None:
            source["flow_queue_meta_name"] = flow_queue_meta_name

        sink = pipeline_config["spec"]["computations"]["Write"]["sinks"]["sink"]
        sink["sink_class_name"] = "NYT::NFlow::TYdb{}{}Sink".format(
            "AtLeastOnce" if at_least_once else "",
            "Framing" if framing else "",
        )
        sink["parameters"].update(
            {
                "endpoint": self.ydb_endpoint,
                "database": self.ydb_database,
                "topic": self.output_topic,
                "write_flow_queue_meta": use_flow_queue_meta,
                "codec": "zstd",
            }
        )
        if sink_attributes_column is not None:
            sink["parameters"]["attributes_column"] = sink_attributes_column

        source_dynamic_parameters = dict(source_dynamic_parameters or {})
        if malformed_message_policy is not None:
            source_dynamic_parameters["malformed_message_policy"] = malformed_message_policy
        if source_dynamic_parameters:
            pipeline_config["dynamic_spec"]["computations"]["Read"]["source_streams"] = {
                "input_topic": {"parameters": source_dynamic_parameters},
            }

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, f"pipeline{name_suffix}.yson")

    def get_flow_view_topic_clusters(self):
        job_manager_state = self.client.get_flow_view(
            self.pipeline_path, view_path="/state/job_manager_state", cache=False
        )
        for name, state in job_manager_state.get("computations", {}).get("Read", {}).items():
            if name.startswith("/sources/input_topic/") and name.endswith("/ydb_info/v0"):
                return state.get("clusters", {})
        return {}

    def get_cached_partitions_count(self, endpoint=None):
        return self.get_cluster_field(endpoint or self.ydb_endpoint, "partition_count")

    def get_cluster_field(self, endpoint, field):
        return self.get_flow_view_topic_clusters().get(endpoint, {}).get(field)

    def read_output_data_messages(self, driver=None):
        driver = driver if driver is not None else self.driver
        messages = read_all_from_topic(driver, self.output_topic, self.check_consumer, detailed=True)
        return [
            message
            for partition_messages in messages.values()
            for message in partition_messages
            if not is_pure_heartbeat(message)
        ]

    def read_output_keys(self, framing=True):
        raw_messages = read_all_from_topic(self.driver, self.output_topic, self.check_consumer)
        messages = (
            unpack_proto_framing(raw_messages)
            if framing
            else [TTestMessage.FromString(message) for message in raw_messages]
        )
        return [get_message_key(message) for message in messages]

    def read_output_attributes(self):
        messages = read_all_from_topic(self.driver, self.output_topic, self.check_consumer, detailed=True)
        return [message.metadata_items for partition_messages in messages.values() for message in partition_messages]

    def prepare_delivery_validation_config(self, lane, record_count, generator_sink_endpoint=None):
        pipeline_config = get_yson_config(DELIVERY_PIPELINE_CONFIG_PATH)
        computations = pipeline_config["spec"]["computations"]
        assert set(computations) == {
            f"{installation}_{role}"
            for installation in DELIVERY_LANES
            for role in ("generator", "observer", "validator")
        }
        computation_ids = {f"{lane}_generator", f"{lane}_observer", f"{lane}_validator"}
        pipeline_config["spec"]["computations"] = computations = {
            computation_id: computation
            for computation_id, computation in computations.items()
            if computation_id in computation_ids
        }
        pipeline_config["spec"]["resources"]["YdbDriver"]["parameters"] = {}
        for computation in computations.values():
            computation["processing_function_parameters"]["run_id"] = DELIVERY_TEST_RUN_ID

        generator = computations[f"{lane}_generator"]
        assert generator["sinks"]["output"]["sink_class_name"] == DELIVERY_SINK_CLASSES[lane]
        generator["source_streams"]["random"]["parameters"]["finite"] = True
        generator["sinks"]["output"]["parameters"].update(
            endpoint=generator_sink_endpoint or self.ydb_endpoint,
            database=self.ydb_database,
            topic=self.output_topic,
        )
        observer_source = computations[f"{lane}_observer"]["source_streams"][f"{lane}_output"]["parameters"]
        observer_source.update(
            endpoint=self.ydb_endpoint,
            database=self.ydb_database,
            topic=self.output_topic,
            consumer=self.input_consumer,
        )
        for computation_id in (f"{lane}_generator", f"{lane}_validator"):
            computations[computation_id]["processing_function_parameters"]["window_size"] = 2000

        used_streams = set()
        for computation in computations.values():
            used_streams.update(computation.get("output_stream_ids", []))
            used_streams.update(computation.get("input_stream_ids", []))
            for sink in computation.get("sinks", {}).values():
                used_streams.update(sink.get("input_stream_ids", []))
        if "streams" in pipeline_config["spec"]:
            pipeline_config["spec"]["streams"] = {
                stream_id: stream
                for stream_id, stream in pipeline_config["spec"]["streams"].items()
                if stream_id in used_streams
            }

        generator_dynamic = pipeline_config["dynamic_spec"]["computations"][f"{lane}_generator"]
        pipeline_config["dynamic_spec"]["computations"] = {f"{lane}_generator": generator_dynamic}
        generator_dynamic["source_streams"]["random"]["parameters"].update(
            partition_count=1,
            partition_message_count=record_count,
            message_count_mean=record_count,
        )
        throttler_id = generator_dynamic["input_rows_throttler_id"]
        pipeline_config["dynamic_spec"]["throttlers"] = {
            throttler_id: {"limit": 10.0, "period": 1000, "request_period": 100}
        }
        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, f"delivery_validation_{lane}.yson")

    def prepare_finite_delivery_validation_config(self, record_count):
        pipeline_config = get_yson_config(self.prepare_delivery_validation_config("alo", record_count))
        computations = pipeline_config["spec"]["computations"]
        del computations["alo_generator"]

        observer = computations["alo_observer"]
        observer["source_streams"]["alo_output"]["parameters"]["finite"] = True
        expected_reader = copy.deepcopy(observer)
        expected_reader["processing_function"] = "NYT::NFlow::NYdbTest::TDeliveryProbeExpectedReader"
        expected_reader["processing_function_parameters"].pop("observed_stream_id")
        expected_reader["processing_function_parameters"].update(expected_stream_id="alo_expected", window_size=2000)
        expected_reader["output_stream_ids"] = ["alo_expected"]
        expected_source = expected_reader["source_streams"].pop("alo_output")
        expected_source["parameters"]["topic"] = self.input_topic
        expected_reader["source_streams"] = {"alo_expected_fixture": expected_source}
        expected_reader["streams_dependency"] = {"alo_expected": ["alo_expected_fixture"]}
        computations["alo_expected_reader"] = expected_reader
        pipeline_config["dynamic_spec"]["computations"] = {}
        pipeline_config["dynamic_spec"]["throttlers"] = {}
        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "delivery_validation_finite.yson")

    def read_delivery_validation_report(self, lane):
        states = self.client.read_states(
            self.pipeline_path,
            computation_id=f"{lane}_validator",
            key={"run_id": DELIVERY_TEST_RUN_ID, "lane": lane},
            name="/validation",
        )
        assert states["errors"] == []
        reports = [state for key_state in states["key_states"] for state in key_state["states"].values()]
        assert len(reports) == 1
        return reports[0]

    def make_delivery_validation_fixture(self, record_count, lane="alo"):
        probe_time = int(time.time()) // 2 * 2 - 5
        payloads = [
            yson.dumps(
                {
                    "run_id": DELIVERY_TEST_RUN_ID,
                    "lane": lane,
                    "producer_id": "finite-producer",
                    "sequence": sequence,
                    "probe_time": probe_time,
                    "bucket_start": probe_time // 2 * 2,
                    "data": f"{DELIVERY_TEST_RUN_ID}/finite-producer/{sequence}",
                }
            )
            for sequence in range(7, 7 + record_count)
        ]
        return payloads, probe_time

    def write_delivery_validation_fixture(self, topic, payloads, probe_time):
        with self.driver.topic_client.writer(topic, codec=ydb.TopicCodec.RAW) as writer:
            metadata = {FLOW_QUEUE_META_NAME: yson.dumps({"event_timestamp": probe_time})}
            batching_write_rows(
                payloads,
                lambda batch: writer.write(
                    [ydb.TopicWriterMessage(data=data, metadata_items=metadata) for data in batch]
                ),
            )
            writer.flush()

    def run_delivery_validation_full_graph(
        self, lane, problems, inject_late_duplicate=False, source_outage=False, publish_proxy=None
    ):
        record_count = 50 if problems else 20
        pipeline_config_path = self.prepare_delivery_validation_config(
            lane,
            record_count,
            generator_sink_endpoint=publish_proxy.endpoint if publish_proxy is not None else None,
        )
        problems_config = ProblemsConfig(interval_seconds=2, problems_max_count=1) if problems else None
        with self.start_flow(
            pipeline_config_path,
            workers_count=2,
            problems=problems,
            worker_problems_config=problems_config,
            controller_problems_config=problems_config,
        ) as federation:
            if publish_proxy is not None:
                self.hold_pending_publication(lane, publish_proxy)
            if source_outage:

                def validator_observed_real_probe():
                    report = self.read_delivery_validation_report(lane)
                    assert report["first_error_kind"] == ""
                    return report["checked_probe_count"] > 0 or any(
                        window["observed_sequences"]
                        for producer in report["producers"]
                        for window in producer["windows"]
                    )

                wait(validator_observed_real_probe, timeout=180, ignore_exceptions=True)
                wait(lambda: self.get_processing_watermark() > 0, timeout=180, ignore_exceptions=True)
                watermark_before = self.get_processing_watermark()
                self.ydb.pause()
                try:
                    time.sleep(3)
                    assert self.get_processing_watermark() == watermark_before
                    assert "delivery_loss" not in str(self.client.get_flow_view(self.pipeline_path, cache=False))
                finally:
                    self.ydb.resume()
            if problems:
                wait(
                    lambda: all(
                        process.injected_problems_count == 1 for process in federation.workers + federation.controllers
                    ),
                    timeout=60,
                )

            def checked_probe_count():
                if not problems:
                    statuses = self.client.get_flow_view(
                        self.pipeline_path, view_path="/feedback/partition_job_statuses", cache=False
                    )
                    for status in statuses.values():
                        error = status.get("current_job_status", {}).get("error", {})
                        assert not error.get("message"), error
                report = self.read_delivery_validation_report(lane)
                assert report["first_error_kind"] == ""
                if any(producer["windows"] for producer in report["producers"]):
                    return 0
                return report["checked_probe_count"]

            wait(lambda: checked_probe_count() == record_count, timeout=180, ignore_exceptions=True)
            if inject_late_duplicate:
                data_messages = [message.data for message in self.read_output_data_messages()]
                with self.driver.topic_client.writer(self.output_topic, codec=ydb.TopicCodec.RAW) as writer:
                    writer.write(data_messages[0])
                    writer.flush()
                self.wait_for_pipeline_error("duplicate_exactly_once_probe", timeout=60)
            else:
                assert "YDB delivery validation failed" not in str(
                    self.client.get_flow_view(self.pipeline_path, cache=False)
                )
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=180)
            report = self.read_delivery_validation_report(lane)
            assert report["checked_probe_count"] == record_count
            assert report["first_error_kind"] == ("duplicate_exactly_once_probe" if inject_late_duplicate else "")
            assert all(not producer["windows"] for producer in report["producers"])
            probes = [yson.loads(message.data) for message in self.read_output_data_messages()]
            assert {probe["sequence"] for probe in probes} == set(range(record_count))
            assert all(probe["run_id"] == DELIVERY_TEST_RUN_ID for probe in probes)
            assert all(probe["lane"] == lane for probe in probes)
            if lane == "exactly_once":
                assert len(probes) == record_count + int(inject_late_duplicate)

    def hold_pending_publication(self, lane, publish_proxy):
        publish_proxy.wait_publish(timeout=180)
        generator_states = self.client.read_states(
            self.pipeline_path,
            computation_id=f"{lane}_generator",
            name="/generator",
        )
        assert generator_states["errors"] == []
        pending_generators = [
            state
            for key_state in generator_states["key_states"]
            for state in key_state["states"].values()
            if state["next_sequence"] > 0
        ]
        assert pending_generators
        pending_window_end = min(state["last_probe_time"] for state in pending_generators) // 2 * 2 + 2
        time.sleep(3)
        pending_report = self.read_delivery_validation_report(lane)
        assert pending_report["checked_probe_count"] == 0
        assert pending_report["first_error_kind"] == ""
        assert self.read_output_data_messages() == []
        assert self.get_processing_watermark() < pending_window_end
        assert "delivery_loss" not in str(self.client.get_flow_view(self.pipeline_path, cache=False))
        publish_proxy.allow_publish()

    @pytest.mark.authors(["pechatnov"])
    def test_delivery_validation_pending_publish_holds_watermark(self):
        with YdbOutageProxy(self.ydb_endpoint, self.port_manager.get_port()) as proxy:
            self.run_delivery_validation_full_graph("exactly_once", False, publish_proxy=proxy)

    @pytest.mark.authors(["pechatnov"])
    @pytest.mark.parametrize("problems", [False, True], ids=["stable", "chaos"])
    def test_delivery_validation_full_graph_alo(self, problems):
        self.run_delivery_validation_full_graph("alo", problems)

    @pytest.mark.authors(["pechatnov"])
    @pytest.mark.parametrize("problems", [False, True], ids=["stable", "chaos"])
    def test_delivery_validation_full_graph_exactly_once(self, problems):
        self.run_delivery_validation_full_graph("exactly_once", problems)

    @pytest.mark.authors(["pechatnov"])
    def test_delivery_validation_subgraphs_are_independent(self):
        lane_b = self.ydb
        lane_a = self.start_ydb("lane-a")
        for topic in (self.input_topic, self.output_topic):
            create_topic(lane_a, topic, self.partition_count, [self.input_consumer, self.check_consumer])
        self.use_instance(lane_a)
        config_a = get_yson_config(self.prepare_delivery_validation_config("alo", 100))
        self.use_instance(lane_b)
        config_b = get_yson_config(self.prepare_delivery_validation_config("alo_second", 20))
        config_a["spec"]["computations"].update(config_b["spec"]["computations"])
        config_a["spec"].setdefault("streams", {}).update(config_b["spec"].get("streams", {}))
        config_a["dynamic_spec"]["computations"].update(config_b["dynamic_spec"]["computations"])
        config_a["dynamic_spec"]["throttlers"].update(config_b["dynamic_spec"]["throttlers"])
        config_a["dynamic_spec"]["throttlers"]["alo_second_generator_rows"]["limit"] = 2.0
        assert len(config_a["spec"]["computations"]) == 6
        for lane in ("alo", "alo_second"):
            validator = config_a["spec"]["computations"][f"{lane}_validator"]
            inputs = [f"{lane}_expected", f"{lane}_observed"]
            assert validator["input_stream_ids"] == inputs
            assert validator["streams_dependency"][f"{lane}_close"] == inputs
            assert validator["streams_dependency"][f"{lane}_status"] == [*inputs, f"{lane}_close", f"{lane}_status"]
        config_path = self.dump_config_to_log_dir(config_a, "delivery_validation_independent.yson")
        self.flow_env = {"YDB_TOKEN": root_token(lane_a)}
        with self.start_flow(config_path, workers_count=2):
            self.wait_jobs_initialized()
            wait(
                lambda: any(
                    producer["windows"] for producer in self.read_delivery_validation_report("alo")["producers"]
                ),
                timeout=180,
                ignore_exceptions=True,
            )
            lane_a.pause()
            try:

                def lane_b_closed():
                    report = self.read_delivery_validation_report("alo_second")
                    return report["checked_probe_count"] == 20 and all(
                        not producer["windows"] for producer in report["producers"]
                    )

                wait(lane_b_closed, timeout=180, ignore_exceptions=True)
                report_a = self.read_delivery_validation_report("alo")
                assert report_a["checked_probe_count"] < 100
                assert any(producer["windows"] for producer in report_a["producers"])
                assert report_a["first_error_kind"] == ""
                assert self.read_delivery_validation_report("alo_second")["first_error_kind"] == ""
            finally:
                lane_a.resume()
            wait(
                lambda: self.read_delivery_validation_report("alo")["checked_probe_count"] == 100,
                timeout=180,
                ignore_exceptions=True,
            )
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=180)
            for lane, driver, count in (("alo", lane_a.driver, 100), ("alo_second", lane_b.driver, 20)):
                probes = [yson.loads(message.data) for message in self.read_output_data_messages(driver)]
                assert {probe["sequence"] for probe in probes} == set(range(count))
                assert all(probe["lane"] == lane for probe in probes)

    @pytest.mark.authors(["pechatnov"])
    def test_delivery_validation_source_outage_holds_watermark(self):
        self.run_delivery_validation_full_graph("alo", False, source_outage=True)

    @pytest.mark.authors(["pechatnov"])
    def test_delivery_validation_rejects_late_exactly_once_duplicate(self):
        self.run_delivery_validation_full_graph("exactly_once", False, inject_late_duplicate=True)

    @pytest.mark.authors(["pechatnov"])
    def test_delivery_validation_finite_closes_last_window_and_cleans_state(self):
        record_count = 20
        payloads, probe_time = self.make_delivery_validation_fixture(record_count)
        for topic in (self.input_topic, self.output_topic):
            self.write_delivery_validation_fixture(topic, payloads, probe_time)

        with self.start_flow(self.prepare_finite_delivery_validation_config(record_count)):
            self.wait_pipeline_state("completed", timeout=180)
            report = self.read_delivery_validation_report("alo")
            assert report["first_error_kind"] == ""
            assert report["checked_probe_count"] == record_count
            assert all(not producer["windows"] for producer in report["producers"])
            assert list(self.client.select_rows(f"* from [{self.pipeline_path}/timers]")) == []

    @pytest.mark.authors(["pechatnov"])
    def test_delivery_validation_finite_reports_totally_missing_probe(self):
        record_count = 20
        payloads, probe_time = self.make_delivery_validation_fixture(record_count)
        self.write_delivery_validation_fixture(self.input_topic, payloads, probe_time)
        self.write_delivery_validation_fixture(self.output_topic, payloads[:-1], probe_time)

        with self.start_flow(self.prepare_finite_delivery_validation_config(record_count)) as federation:
            self.wait_for_pipeline_error("delivery_loss", timeout=180)

            def durable_loss_report_ready():
                current = self.read_delivery_validation_report("alo")
                return current["first_error_kind"] == "delivery_loss" and all(
                    not producer["windows"] for producer in current["producers"]
                )

            wait(durable_loss_report_ready, timeout=180, ignore_exceptions=True)
            report = self.read_delivery_validation_report("alo")
            partitions = self.client.get_flow_view(
                self.pipeline_path, view_path="/state/execution_spec/layout/partitions", cache=False
            )
            reporter_partitions = {
                partition_id
                for partition_id, partition in partitions.items()
                if partition["computation_id"] == "alo_validator"
            }
            old_error_jobs = {}

            def current_error_job_ready():
                statuses = self.client.get_flow_view(
                    self.pipeline_path, view_path="/feedback/partition_job_statuses", cache=False
                )
                old_error_jobs.clear()
                old_error_jobs.update(
                    {
                        partition_id: statuses[partition_id]["current_job_id"]
                        for partition_id in reporter_partitions
                        if statuses.get(partition_id, {}).get("current_job_id")
                        and "delivery_loss"
                        in str(statuses[partition_id].get("current_job_status", {}).get("retryable_errors", []))
                    }
                )
                return len(old_error_jobs) == 1

            wait(current_error_job_ready, timeout=180, ignore_exceptions=True)
            reporter_partitions = set(old_error_jobs)
            old_job_ids = set(old_error_jobs.values())
            federation.workers[0].restart()

            def restored_reporter_reasserted():
                current_statuses = self.client.get_flow_view(
                    self.pipeline_path, view_path="/feedback/partition_job_statuses", cache=False
                )
                new_job_ids = {
                    current_statuses[partition_id]["current_job_id"]
                    for partition_id in reporter_partitions
                    if partition_id in current_statuses
                }
                if len(new_job_ids) != len(reporter_partitions) or not new_job_ids.isdisjoint(old_job_ids):
                    return False
                return any(
                    "delivery_loss" in str(current_statuses[partition_id]["current_job_status"]["retryable_errors"])
                    for partition_id in reporter_partitions
                )

            wait(restored_reporter_reasserted, timeout=180, ignore_exceptions=True)
            assert self.read_delivery_validation_report("alo") == report
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=180)
            assert len(read_all_from_topic(self.driver, self.output_topic, self.check_consumer)) == record_count - 1

    @pytest.mark.authors(["pechatnov"])
    def test_deferred_publication_cancel_reuses_sequence(self):
        result = self.ydb._run_probe("deferred-cancel-reuse")
        assert result["status"] == "SUCCESS"
        assert result["operations"]["cancelledInitSeqNo"] == 0
        assert result["operations"]["publishedInitSeqNo"] == 0
        assert result["operations"]["visible"] == ["published"]

    @pytest.mark.authors(["pechatnov"])
    @pytest.mark.parametrize("framing", [False, True], ids=["plain", "framing"])
    @pytest.mark.parametrize("problems", [False, True], ids=["regular", "chaos"])
    def test_deferred_publications(self, framing, problems):
        self.run_deferred_publications(framing, problems)

    @pytest.mark.authors(["pechatnov"])
    @pytest.mark.parametrize("ordered_source", ["swift", "transform"])
    def test_ordered_source_deferred_publications(self, ordered_source):
        self.run_deferred_publications(framing=True, problems=True, ordered_source=ordered_source)

    @pytest.mark.authors(["pechatnov"])
    def test_deferred_publication_selects_writable_cluster(self):
        with FederationDiscoveryMock(
            [
                {
                    "name": "readonly",
                    "endpoint": f"127.0.0.1:{self.port_manager.get_port()}",
                    "path": self.ydb_database,
                    "status": "READ_ONLY",
                },
                {
                    "name": "writable",
                    "endpoint": self.ydb_endpoint,
                    "path": self.ydb_database,
                    "status": "AVAILABLE",
                },
            ],
            self.port_manager.get_port(),
        ) as discovery:
            self.run_deferred_publications(framing=False, problems=False, discovery=discovery)

    @pytest.mark.authors(["pechatnov"])
    @pytest.mark.parametrize("outage_stage", ["publish", "write_ack"])
    def test_deferred_publication_switches_cluster_between_waves(self, outage_stage):
        clusters = []
        provision_flow_login(self.ydb)
        for name in ("a", "b"):
            instance = self.start_ydb(f"failover-{name}", deferred_publish=True)
            provision_flow_login(instance)
            create_topic(instance, self.output_topic, self.partition_count, [self.input_consumer, self.check_consumer])
            clusters.append(instance)
        self.flow_env = {FLOW_LOGIN_PASSWORD_ENV_VAR: FLOW_LOGIN_PASSWORD}
        proxy = self.stack.enter_context(
            YdbOutageProxy(
                clusters[0].endpoint,
                self.port_manager.get_port(),
                on_first_publish=clusters[0].pause if outage_stage == "publish" else None,
                on_first_write_ack=clusters[0].pause if outage_stage == "write_ack" else None,
            )
        )
        discovery = self.stack.enter_context(
            FederationDiscoveryMock(
                [
                    {"name": "a", "endpoint": proxy.endpoint, "path": clusters[0].database, "status": "AVAILABLE"},
                    {
                        "name": "b",
                        "endpoint": clusters[1].endpoint,
                        "path": clusters[1].database,
                        "status": "READ_ONLY",
                    },
                ],
                self.port_manager.get_port(),
            )
        )
        pipeline_config = get_yson_config(
            self.prepare_pipeline_config(
                finite=False,
                framing=False,
                use_flow_queue_meta=False,
                sink_attributes_column="attributes",
            )
        )
        pipeline_config["spec"]["resources"]["YdbDriverResource"]["parameters"].update(
            {"auth_mode": "login", "username": FLOW_LOGIN_USER, "password_env_var": FLOW_LOGIN_PASSWORD_ENV_VAR}
        )
        sink = pipeline_config["spec"]["computations"]["Write"]["sinks"]["sink"]
        sink["sink_class_name"] = "NYT::NFlow::TYdbTwoPhaseCommitSink"
        sink["parameters"].update(
            {
                "endpoint": discovery.endpoint,
                "database": clusters[0].database,
                "update_partition_count_period": 100,
            }
        )
        pipeline_config["dynamic_spec"]["computations"]["Write"].setdefault("parameters", {})[
            "desired_partition_count"
        ] = 1
        if outage_stage == "write_ack":
            pipeline_config["spec"]["computations"]["Write"]["distribution_ordering"] = "relaxed"
            pipeline_config["dynamic_spec"]["computations"]["Write"]["sinks"]["sink"]["parameters"].update(
                {"preparation_attempt_timeout": 2000, "preparation_target_backoff": 1000}
            )
        pipeline_config_path = self.dump_config_to_log_dir(pipeline_config, "pipeline_deferred_failover.yson")

        def read_keys(instance):
            return sorted(
                get_message_key(TTestMessage.FromString(message))
                for message in read_all_from_topic(instance.driver, self.output_topic, self.check_consumer)
            )

        def get_message_count(instance):
            info = instance.driver.topic_client.describe_topic(self.output_topic, include_stats=True)
            return sum(
                partition.partition_stats.partition_end - partition.partition_stats.partition_start
                for partition in info.partitions
            )

        def get_write_job_ids():
            partitions = self.client.get_flow_view(
                self.pipeline_path, view_path="/state/execution_spec/layout/partitions", cache=False
            )
            statuses = self.client.get_flow_view(
                self.pipeline_path, view_path="/feedback/partition_job_statuses", cache=False
            )
            write_partition_ids = {
                partition_id for partition_id, partition in partitions.items() if partition["computation_id"] == "Write"
            }
            write_job_ids = {
                statuses.get(partition_id, {}).get("current_job_id") for partition_id in write_partition_ids
            }
            if not write_partition_ids or not all(write_job_ids) or len(write_job_ids) != len(write_partition_ids):
                return set()
            return write_job_ids

        with self.start_flow(pipeline_config_path) as federation:
            self.wait_jobs_initialized()
            ordering_warning = "Strict distribution ordering with federated topic publication"
            if outage_stage == "publish":
                wait(
                    lambda: ordering_warning
                    in str(self.client.flow_execute(self.pipeline_path, flow_command="describe-pipeline")),
                    timeout=60,
                )
            else:
                description = self.client.flow_execute(self.pipeline_path, flow_command="describe-pipeline")
                assert ordering_warning not in str(description)
            initial_write_job_ids = get_write_job_ids()
            assert len(initial_write_job_ids) == 1
            expected_a = ["a-0"]
            self.fill_input_topic(key_prefix="a-", record_count=1)
            if outage_stage == "publish":
                proxy.wait_publish(timeout=180)
            else:
                proxy.wait_write_ack(timeout=180)
            assert get_message_count(clusters[1]) == 0

            discovery.set_status("a", "READ_ONLY")
            discovery.set_status("b", "AVAILABLE")
            expected_b = ["b1-0", "b2-0"]
            if outage_stage == "write_ack":
                expected_b = sorted(expected_a + expected_b)
            self.fill_input_topic(key_prefix="b1-", record_count=1)
            first_b_count = 1 + (outage_stage == "write_ack")
            wait(lambda: get_message_count(clusters[1]) == first_b_count, timeout=180)
            self.fill_input_topic(key_prefix="b2-", record_count=1)
            wait(lambda: get_message_count(clusters[1]) == first_b_count + 1, timeout=180)

            assert get_write_job_ids() == initial_write_job_ids
            federation.workers[0].restart()

            def write_job_restarted():
                current_write_job_ids = get_write_job_ids()
                return len(current_write_job_ids) == 1 and current_write_job_ids.isdisjoint(initial_write_job_ids)

            wait(write_job_restarted, timeout=180)
            self.wait_jobs_initialized()
            self.fill_input_topic(key_prefix="c-", record_count=1)
            wait(lambda: get_message_count(clusters[1]) == first_b_count + 2, timeout=180)

            clusters[0].resume()
            proxy.allow_publish()
            discovery.set_status("a", "AVAILABLE")
            expected_a_count = 1 if outage_stage == "publish" else 0
            wait(lambda: get_message_count(clusters[0]) == expected_a_count, timeout=180)
            if outage_stage == "write_ack":
                discovery.set_status("b", "READ_ONLY")
                self.fill_input_topic(key_prefix="d-", record_count=1)
                wait(lambda: get_message_count(clusters[0]) == 1, timeout=180)
                expected_a = ["d-0"]
            assert read_keys(clusters[0]) == expected_a
            assert read_keys(clusters[1]) == sorted(expected_b + ["c-0"])

    def run_deferred_publications(self, framing, problems, ordered_source=None, discovery=None):
        record_count = 100
        self.fill_input_topic(record_count=record_count)
        pipeline_config = get_yson_config(
            self.prepare_pipeline_config(
                framing=framing,
                use_flow_queue_meta=discovery is None,
                sink_attributes_column=None if framing else "attributes",
            )
        )
        sink_computation = "Write"
        if ordered_source is not None:
            sink_computation = "Read"
            computations = pipeline_config["spec"]["computations"]
            sink = computations["Write"]["sinks"]["sink"]
            sink["input_stream_ids"] = ["data"]
            computations["Read"]["sinks"] = {"sink": sink}
            if ordered_source == "transform":
                computations["Read"]["computation_class_name"] = "TTransformSourceReader"
            del computations["Write"]
            del pipeline_config["spec"]["streams"]["data_2"]
            dynamic_computations = pipeline_config["dynamic_spec"]["computations"]
            dynamic_computations["Read"]["sinks"] = dynamic_computations["Write"]["sinks"]
            del dynamic_computations["Write"]
        sink = pipeline_config["spec"]["computations"][sink_computation]["sinks"]["sink"]
        sink["sink_class_name"] = "NYT::NFlow::TYdbTwoPhaseCommitSink"
        if discovery is not None:
            sink["parameters"]["endpoint"] = discovery.endpoint
        if framing:
            sink["parameters"]["framing_format"] = "lenval"
            pipeline_config["dynamic_spec"]["computations"][sink_computation]["sinks"]["sink"]["parameters"][
                "max_bytes_per_batch"
            ] = 1
        pipeline_config_path = self.dump_config_to_log_dir(pipeline_config, "pipeline_deferred.yson")
        with self.start_flow(
            pipeline_config_path,
            workers_count=4,
            controllers_count=2,
            problems=problems,
            worker_problems_config=ProblemsConfig(interval_seconds=5, problems_max_count=2) if problems else None,
            controller_problems_config=ProblemsConfig(interval_seconds=5, problems_max_count=1) if problems else None,
        ) as federation:
            if problems:
                wait(lambda: sum(worker.injected_problems_count for worker in federation.workers) > 0, timeout=60)
            self.wait_pipeline_state("completed", timeout=300)
            if not problems:
                assert all(
                    process.injected_problems_count == 0 for process in federation.workers + federation.controllers
                )
            data_messages = self.read_output_data_messages()
            assert discovery is not None or all(
                message.metadata_items.get(FLOW_QUEUE_META_NAME) is not None for message in data_messages
            )
            if framing:
                unpacked = []
                for message in data_messages:
                    batch = unpack_proto_framing([message.data])
                    assert len(batch) == 1
                    unpacked.extend(batch)
            else:
                unpacked = [TTestMessage.FromString(message.data) for message in data_messages]
                assert all(message.metadata_items["pipeline"] == PIPELINE_ATTRIBUTE for message in data_messages)
                assert all("key" in message.metadata_items for message in data_messages)
            actual = [get_message_key(message) for message in unpacked]
            assert sorted(actual) == sorted(row["Key"] for row in generate_log(record_count))
            for table in ("compact_output_messages", "compact_partition_output_messages"):
                assert list(self.client.select_rows(f"* from [{self.pipeline_path}/{table}]")) == []
            for table in ("partition_states", "states"):
                states = self.client.select_rows(f"* from [{self.pipeline_path}/{table}]")
                assert not any(row["name"] == DEFERRED_PUBLICATION_STATE_NAME for row in states)

    @pytest.mark.authors(["pechatnov"])
    def test_hard_commit_preserves_read_session(self):
        record_count = 10
        with self.driver.topic_client.writer(self.input_topic, partition_id=0) as writer:
            writer.write(pack_proto_framing([TTestMessage(**row) for row in generate_log(record_count)], batch_size=1))
            writer.flush()

        def consumer_stats():
            description = self.driver.topic_client.describe_consumer(
                self.input_topic, self.input_consumer, include_stats=True
            )
            return next(p.partition_consumer_stats for p in description.partitions if p.partition_id == 0)

        source_parameters = {"partition_filter": [[0, 1]]}
        with self.start_flow(
            self.prepare_pipeline_config(finite=False, source_parameters=source_parameters)
        ) as federation:
            wait(lambda: len(self.read_output_keys()) == record_count, timeout=180, ignore_exceptions=True)
            wait(lambda: consumer_stats().committed_offset == record_count, timeout=60, ignore_exceptions=True)
            old_session_id = consumer_stats().read_session_id
            assert old_session_id

            def source_job_count():
                response = requests.get(
                    f"http://localhost:{federation.workers[0].monitoring_port}/solomon_proxy/sensors", timeout=5
                )
                response.raise_for_status()
                return next(
                    (
                        sensor["value"]
                        for sensor in response.json()["sensors"]
                        if sensor["labels"].get("sensor") == "yt.flow.worker.job_count"
                        and sensor["labels"].get("computation_id") == "Read"
                    ),
                    None,
                )

            wait(lambda: (source_job_count() or 0) > 0, timeout=60)
            self.client.pause_pipeline(self.pipeline_path)
            self.wait_pipeline_state("paused")
            wait(lambda: source_job_count() == 0, timeout=60)

            dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)["spec"]
            source_streams = dynamic_spec["computations"]["Read"].setdefault("source_streams", {})
            parameters = source_streams.setdefault("input_topic", {}).setdefault("parameters", {})
            parameters.update(
                {
                    "read_from_instant": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
                    "commit_timeout": 10000,
                }
            )
            self.client.set_pipeline_dynamic_spec(self.pipeline_path, dynamic_spec)
            self.driver.topic_client.commit_offset(self.input_topic, self.input_consumer, 0, 0)
            self.client.start_pipeline(self.pipeline_path)
            self.wait_pipeline_state("working")

            def has_restarted_session():
                stats = consumer_stats()
                return bool(stats.read_session_id) and stats.read_session_id != old_session_id

            wait(has_restarted_session, timeout=60, ignore_exceptions=True)
            before = consumer_stats()
            assert before.committed_offset == 0
            assert before.read_session_id != old_session_id
            wait(lambda: consumer_stats().committed_offset == record_count, timeout=60, ignore_exceptions=True)
            after = consumer_stats()
            assert after.read_session_id == before.read_session_id
            assert sorted(self.read_output_keys()) == sorted(row["Key"] for row in generate_log(record_count))

            self.driver.topic_client.commit_offset(
                self.input_topic, self.input_consumer, 0, 0, read_session_id=after.read_session_id
            )
            assert consumer_stats().committed_offset == record_count
            with pytest.raises(ydb.Error, match="already dead session"):
                self.driver.topic_client.commit_offset(
                    self.input_topic, self.input_consumer, 0, 0, read_session_id=old_session_id
                )
            current = consumer_stats()
            assert current.committed_offset == record_count
            assert current.read_session_id == after.read_session_id

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count", "problems", "not_zero_start_offset", "at_least_once"),
        [
            pytest.param(1, 1, False, False, False, id="1c_1w_stable"),
            pytest.param(4, 2, True, False, False, id="2c_4w_unstable"),
            pytest.param(1, 1, False, True, False, id="1c_1w_not_zero_start_offset"),
            pytest.param(4, 2, True, False, True, id="2c_4w_unstable_at_least_once"),
        ],
    )
    def test_basic(self, workers_count, controllers_count, problems, not_zero_start_offset, at_least_once):
        self.fill_input_topic()
        if not_zero_start_offset:
            read_all_from_topic(self.driver, self.input_topic, self.input_consumer, commit=True)
            self.fill_input_topic()

        with self.start_flow(
            self.prepare_pipeline_config(at_least_once=at_least_once),
            workers_count=workers_count,
            controllers_count=controllers_count,
            problems=problems,
        ):
            self.wait_pipeline_state("completed", timeout=180)
            expected = {}
            for row in generate_log():
                expected.setdefault(row["Key"], []).append(row)
            raw_messages = read_all_from_topic(self.driver, self.output_topic, self.check_consumer)
            got_rows = [get_message_row(message) for message in unpack_proto_framing(raw_messages)]
            got = {}
            for row in got_rows:
                got.setdefault(row["Key"], []).append(row)
            if not at_least_once:
                assert len(expected) == len(got_rows)
                assert expected == got
            else:
                assert len(expected) <= len(got_rows)
                assert {key: {row["Data"] for row in rows} for key, rows in expected.items()} == {
                    key: {row["Data"] for row in rows} for key, rows in got.items()
                }

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize("meta_name", [FLOW_QUEUE_META_NAME, LEGACY_FLOW_QUEUE_META_NAME])
    def test_flow_queue_meta(self, meta_name):
        pipeline_config_path = self.prepare_pipeline_config(
            finite=False, use_flow_queue_meta=True, name_suffix=f"_{meta_name}"
        )
        with self.start_flow(pipeline_config_path):
            wait(
                lambda: self.client.get_pipeline_state(self.pipeline_path) == "working",
                timeout=180,
                ignore_exceptions=True,
            )
            wait(lambda: self.get_processing_watermark() < EVENT_WATERMARK, timeout=10, ignore_exceptions=True)
            for watermark in (EVENT_WATERMARK, EVENT_WATERMARK + 100):
                self.write_heartbeats(watermark, meta_name)
                wait(lambda: self.get_processing_watermark() == watermark, timeout=180, ignore_exceptions=True)
                wait(lambda: self.get_heartbeat_watermark() == watermark, timeout=20, ignore_exceptions=True)

            messages = read_all_from_topic(self.driver, self.output_topic, self.check_consumer, detailed=True)
            heartbeat_producers = {}
            for partition_id, partition_messages in messages.items():
                for message in partition_messages:
                    if is_pure_heartbeat(message):
                        heartbeat_producers.setdefault(partition_id, set()).add(message.producer_id)
            assert heartbeat_producers
            bases = set()
            for partition_id, producers in heartbeat_producers.items():
                assert len(producers) == 1
                base, _, suffix = next(iter(producers)).rpartition("-")
                assert suffix == str(partition_id)
                bases.add(base)
            assert len(bases) == 1

    ATTRIBUTE_TEST_RECORD_COUNT = 20

    @pytest.mark.authors(["pechatnov"])
    @pytest.mark.parametrize("at_least_once", [False, True])
    def test_sink_attributes(self, at_least_once):
        self.fill_input_topic(record_count=self.ATTRIBUTE_TEST_RECORD_COUNT)
        pipeline_config_path = self.prepare_pipeline_config(
            at_least_once=at_least_once,
            framing=False,
            sink_attributes_column="attributes",
            name_suffix=f"_sink_attributes_{at_least_once}",
        )
        with self.start_flow(pipeline_config_path):
            self.wait_pipeline_state("completed", timeout=180)
            expected_keys = {row["Key"] for row in generate_log(self.ATTRIBUTE_TEST_RECORD_COUNT)}
            attributes = self.read_output_attributes()
            if at_least_once:
                assert len(attributes) >= len(expected_keys)
            else:
                assert len(attributes) == len(expected_keys)
            assert {attribute["key"].decode() for attribute in attributes} == expected_keys
            assert all(attribute["pipeline"] == PIPELINE_ATTRIBUTE for attribute in attributes)
            assert {attribute[KAFKA_KEY_ATTRIBUTE_NAME].decode() for attribute in attributes} == expected_keys

    @pytest.mark.authors(["pechatnov"])
    def test_source_attributes(self):
        self.fill_input_topic(
            metadata_items={
                "user_attribute": b"value",
                "custom_flow_queue_meta": b"custom-value",
                FLOW_QUEUE_META_NAME: b"{}",
                LEGACY_FLOW_QUEUE_META_NAME: b"{}",
                KAFKA_KEY_ATTRIBUTE_NAME: b"kafka-key",
                "__delay_seconds": b"1",
            },
            record_count=self.ATTRIBUTE_TEST_RECORD_COUNT,
        )
        pipeline_config_path = self.prepare_pipeline_config(
            framing=False,
            source_attributes_column="attributes",
            sink_attributes_column="attributes",
            try_parse_flow_queue_meta=False,
            flow_queue_meta_name="custom_flow_queue_meta",
            name_suffix="_source_attributes",
        )
        with self.start_flow(pipeline_config_path):
            self.wait_pipeline_state("completed", timeout=180)
            attributes = self.read_output_attributes()
            assert len(attributes) == self.ATTRIBUTE_TEST_RECORD_COUNT
            assert all(
                attribute
                == {
                    "custom_flow_queue_meta": b"custom-value",
                    "user_attribute": b"value",
                    KAFKA_KEY_ATTRIBUTE_NAME: b"kafka-key",
                }
                for attribute in attributes
            )

    @pytest.mark.authors(["pechatnov"])
    def test_partition_change_and_cache(self):
        with self.start_flow(self.prepare_pipeline_config(finite=False, name_suffix="_0")):
            wait(lambda: self.get_cached_partitions_count() == self.partition_count, timeout=180)
            self.partition_count = 7
            ydb.retry_operation_sync(
                lambda: self.driver.topic_client.alter_topic(
                    self.input_topic, set_min_active_partitions=self.partition_count
                )
            )
            wait(lambda: self.get_cached_partitions_count() == self.partition_count, timeout=300)

        pipeline_config_path = self.prepare_pipeline_config(
            finite=True, disable_partitions_cache_updating=True, name_suffix="_1"
        )
        self.fill_input_topic()
        with self.start_flow(pipeline_config_path):
            self.wait_pipeline_state("completed", timeout=180)

    @pytest.mark.authors(["pechatnov"])
    def test_borrows_partition_count_from_answering_cluster(self):
        dead_endpoint = f"127.0.0.1:{self.port_manager.get_port()}"
        with FederationDiscoveryMock(
            [
                {"name": "dc1", "endpoint": self.ydb_endpoint, "path": self.ydb_database, "status": "AVAILABLE"},
                {"name": "dc2", "endpoint": dead_endpoint, "path": self.ydb_database, "status": "AVAILABLE"},
            ],
            self.port_manager.get_port(),
        ) as discovery:
            pipeline_config_path = self.prepare_pipeline_config(
                finite=False,
                source_parameters={"endpoint": discovery.endpoint, "start_session_timeout": 5000},
            )
            with self.start_flow(pipeline_config_path):
                wait(
                    lambda: self.client.get_pipeline_state(self.pipeline_path) == "working",
                    timeout=180,
                    ignore_exceptions=True,
                )
                wait(
                    lambda: self.get_cached_partitions_count(self.ydb_endpoint) == self.partition_count,
                    timeout=120,
                    ignore_exceptions=True,
                )
                self.fill_input_topic(key_prefix="borrowed_")
                wait(
                    lambda: sum(key.startswith("borrowed_") for key in self.read_output_keys()) >= EVENT_COUNT,
                    timeout=180,
                    ignore_exceptions=True,
                )
                assert self.get_cached_partitions_count(dead_endpoint) is None

    @pytest.mark.authors(["pechatnov"])
    def test_federation_read_only_cluster(self):
        with FederationDiscoveryMock(
            [{"name": "dc1", "endpoint": self.ydb_endpoint, "path": self.ydb_database, "status": "AVAILABLE"}],
            self.port_manager.get_port(),
        ) as discovery:
            pipeline_config_path = self.prepare_pipeline_config(
                finite=False,
                source_parameters={"endpoint": discovery.endpoint},
            )
            with self.start_flow(pipeline_config_path):
                wait(
                    lambda: self.client.get_pipeline_state(self.pipeline_path) == "working",
                    timeout=180,
                    ignore_exceptions=True,
                )
                wait(
                    lambda: self.get_cluster_field(self.ydb_endpoint, "partition_count") == self.partition_count,
                    timeout=120,
                    ignore_exceptions=True,
                )
                assert self.get_cluster_field(self.ydb_endpoint, "read_only_at") is None

                def read_only_at():
                    return self.get_cluster_field(self.ydb_endpoint, "read_only_at")

                def wait_read_only_at_refreshes_past(previous):
                    wait(lambda: (read_only_at() or "") > (previous or ""), timeout=120, ignore_exceptions=True)

                for status, key_prefix in (("READ_ONLY", "ro_"), ("UNAVAILABLE", "unav_")):
                    discovery.set_status("dc1", status)
                    wait_read_only_at_refreshes_past(read_only_at())
                    wait_read_only_at_refreshes_past(read_only_at())
                    assert self.get_cluster_field(self.ydb_endpoint, "partition_count") == self.partition_count
                    self.fill_input_topic(key_prefix=key_prefix)
                    wait(
                        lambda: sum(key.startswith(key_prefix) for key in self.read_output_keys()) >= EVENT_COUNT,
                        timeout=180,
                        ignore_exceptions=True,
                    )

                discovery.set_status("dc1", "AVAILABLE")

                def stopped_refreshing():
                    value = read_only_at()
                    time.sleep(10)
                    return read_only_at() == value

                wait(stopped_refreshing, timeout=180, ignore_exceptions=True)
                assert read_only_at() is not None

    @pytest.mark.authors(["pechatnov"])
    @pytest.mark.skipif(
        yatest.common.context.sanitize == "thread",
        reason="The kill/decay scenario with its fixed multi-minute waits does not fit the TSAN slowdown",
    )
    def test_dead_cluster_watermark(self):
        dc1 = self.start_ydb("dead-dc1")
        dc2 = self.start_ydb("dead-dc2")
        for instance in (dc1, dc2):
            create_topic(instance, self.input_topic, self.partition_count, [self.input_consumer])
        with FederationDiscoveryMock(
            [
                {"name": "dc1", "endpoint": dc1.endpoint, "path": dc1.database, "status": "AVAILABLE"},
                {"name": "dc2", "endpoint": dc2.endpoint, "path": dc2.database, "status": "AVAILABLE"},
            ],
            self.port_manager.get_port(),
        ) as discovery:
            pipeline_config_path = self.prepare_pipeline_config(
                finite=False,
                use_flow_queue_meta=True,
                source_parameters={
                    "endpoint": discovery.endpoint,
                    "database": dc1.database,
                    "start_session_timeout": 5000,
                },
                source_dynamic_parameters={"unavailable_threshold": 2000},
            )
            with self.start_flow(pipeline_config_path):
                wait(
                    lambda: self.client.get_pipeline_state(self.pipeline_path) == "working",
                    timeout=180,
                    ignore_exceptions=True,
                )
                for endpoint in (dc1.endpoint, dc2.endpoint):
                    wait(
                        lambda: self.get_cached_partitions_count(endpoint) == self.partition_count,
                        timeout=120,
                        ignore_exceptions=True,
                    )

                def push_fresh(instances):
                    for instance in instances:
                        self.write_heartbeats(int(time.time()), driver=instance.driver)
                    return self.get_processing_watermark()

                def count_output_keys(prefix):
                    return sum(key.startswith(prefix) for key in self.read_output_keys())

                self.fill_input_topic(driver=dc1.driver, key_prefix="dc1_")
                self.fill_input_topic(driver=dc2.driver, key_prefix="dc2_")
                wait(
                    lambda: count_output_keys("dc1_") >= EVENT_COUNT and count_output_keys("dc2_") >= EVENT_COUNT,
                    timeout=180,
                    ignore_exceptions=True,
                )
                wait(
                    lambda: 0 < push_fresh([dc1, dc2]) and int(time.time()) - push_fresh([dc1, dc2]) <= 180,
                    timeout=180,
                    ignore_exceptions=True,
                )

                kill_time = int(time.time())
                dc2.stop()

                def push_fresh_dc1():
                    return push_fresh([dc1])

                wait(lambda: push_fresh_dc1() > kill_time + 10, timeout=300, ignore_exceptions=True)
                watermark_a = push_fresh_dc1()
                time_a = int(time.time())
                deadline = time.time() + 25
                while time.time() < deadline:
                    push_fresh_dc1()
                    time.sleep(2)
                watermark_b = push_fresh_dc1()
                elapsed = int(time.time()) - time_a
                assert (
                    watermark_b - watermark_a >= elapsed - 10
                ), f"watermark stalled while a cluster was dead: {watermark_a=}, {watermark_b=}, {elapsed=}"

                self.fill_input_topic(driver=dc1.driver, key_prefix="alive_dc1_")
                wait(lambda: count_output_keys("alive_dc1_") >= EVENT_COUNT, timeout=180, ignore_exceptions=True)

    def write_corrupted_messages(self):
        for partition_id in range(self.partition_count):
            with self.driver.topic_client.writer(
                self.input_topic, partition_id=partition_id, codec=ydb.TopicCodec.RAW
            ) as writer:
                writer.write(b"not_valid_framing")
                writer.flush()

    @pytest.mark.authors(["mishatag"])
    @pytest.mark.parametrize("malformed_message_policy", ["fail", "drop", "keep"])
    def test_malformed_message(self, malformed_message_policy):
        if malformed_message_policy != "fail":
            self.fill_input_topic()
        self.write_corrupted_messages()
        pipeline_config_path = self.prepare_pipeline_config(
            malformed_message_policy=malformed_message_policy,
            name_suffix=f"_malformed_{malformed_message_policy}",
        )
        with self.start_flow(pipeline_config_path):
            if malformed_message_policy == "fail":
                self.wait_for_pipeline_error("Failed to process topic message")
                return
            self.wait_pipeline_state("completed", timeout=180)
            raw_messages = read_all_from_topic(self.driver, self.output_topic, self.check_consumer)
            got_rows = [get_message_row(message) for message in unpack_proto_framing(raw_messages)]
            valid_rows = [row for row in got_rows if row["Key"] != "unparsed"]
            unparsed_rows = [row for row in got_rows if row["Key"] == "unparsed"]
            expected = {}
            for row in generate_log():
                expected.setdefault(row["Key"], []).append(row)
            got = {}
            for row in valid_rows:
                got.setdefault(row["Key"], []).append(row)
            assert len(valid_rows) == len(expected)
            assert expected == got
            if malformed_message_policy == "drop":
                assert unparsed_rows == []
            else:
                assert len(unparsed_rows) == self.partition_count
                assert all(row["Data"] == "not_valid_framing" for row in unparsed_rows)


class TestYdbProductionFlowServer(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path("yt/yt/flow/bin/flow_server/flow_server")

    def setup_method(self, method):
        super().setup_method(method)
        self.stack = ExitStack()
        try:
            self.ydb = PublicYdbFixtures(self.port_manager, self.test_name).start(self.stack, "main")
            self.input_topic = f"{self.test_name}-input"
            self.output_topic = f"{self.test_name}-output"
            self.consumer = f"{self.test_name}-consumer"
            for topic in (self.input_topic, self.output_topic):
                create_topic(self.ydb, topic, 1, [self.consumer])
            run_yt_sync("primary", self.work_yt_path)
        except BaseException:
            self.stack.close()
            raise

    def teardown_method(self, method):
        self.stack.close()
        super().teardown_method(method)

    def prepare_pipeline_config(self):
        address = {"endpoint": self.ydb.endpoint, "database": self.ydb.database}
        pipeline_config = {
            "spec": {
                "computations": {
                    "reader": {
                        "computation_class_name": "NYT::NFlow::TSwiftPassthroughOrderedSourceComputation",
                        "input_stream_ids": [],
                        "output_stream_ids": ["data"],
                        "source_streams": {
                            "topic": {
                                "source_class_name": "NYT::NFlow::TYdbSource",
                                "parameters": {
                                    **address,
                                    "topic": self.input_topic,
                                    "consumer": self.consumer,
                                    "finite": True,
                                },
                            },
                        },
                        "sinks": {
                            "topic": {
                                "sink_class_name": "NYT::NFlow::TYdbSink",
                                "input_stream_ids": ["data"],
                                "parameters": {**address, "topic": self.output_topic, "payload_column": "data"},
                            },
                        },
                        "required_resource_ids": {"YdbDriverResource": {"alias": "YdbDriver"}},
                    },
                },
                "streams": {
                    "data": {
                        "schema": [
                            {"name": "data", "type": "string"},
                            {"name": "unparsed", "type": "any"},
                            {"name": "offset", "type": "int64"},
                            {"name": "sub_offset", "type": "int64"},
                        ],
                    },
                },
                "resources": {
                    "YdbDriverResource": {
                        "resource_class_name": "NYT::NFlow::TYdbTopicDriver",
                        "parameters": {},
                        "dependencies": {},
                    },
                },
            },
            "dynamic_spec": {"computations": {"reader": {}}, "job_tracker": {"job_threads": 4}},
        }
        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline_flow_server.yson")

    @pytest.mark.authors(["pechatnov"])
    def test_production_server_runs_public_source_and_sink(self):
        payloads = [f"production-{index}".encode() for index in range(10)]
        with self.ydb.driver.topic_client.writer(self.input_topic, codec=ydb.TopicCodec.RAW) as writer:
            writer.write(payloads)
            writer.flush()
        with self.start_flow_process_federation(
            node_config={"enable_porto_resource_tracker": False},
            pipeline_binary_args={"--config": self.prepare_pipeline_config()},
            workers_count=1,
            controllers_count=1,
            additional_env={"YDB_TOKEN": root_token(self.ydb)},
        ):
            self.wait_pipeline_state("completed", timeout=240)
        assert sorted(read_all_from_topic(self.ydb.driver, self.output_topic, self.consumer)) == sorted(payloads)
