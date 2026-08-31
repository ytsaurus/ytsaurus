import datetime
import logging
import typing

import pytest
import yatest.common
import yt.wrapper
from yt.common import wait
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_SWIFT_CONFIG_PATH = yatest.common.source_path(
    f"{yatest.common.context.project_path}/pipeline/pipeline_swift.yson"
)
PIPELINE_TRANSFORM_CONFIG_PATH = yatest.common.source_path(
    f"{yatest.common.context.project_path}/pipeline/pipeline_transform.yson"
)

if yatest.common.context.sanitize is not None:
    EVENT_COUNT = 200
else:
    EVENT_COUNT = 1000

MIGRATION_EVENT_COUNT = EVENT_COUNT
V1_SOURCE_CLASS = "NYT::NFlow::NStaticTableConnector::TSource"
V2_SOURCE_CLASS = "NYT::NFlow::NStaticTableConnectorV2::TSource"


class TableInfo:
    def __init__(self, alias, create_time, event_count, input_dir):
        self.alias = alias
        self.create_time = create_time
        self.path = input_dir + "/" + datetime.datetime.utcfromtimestamp(create_time).isoformat()

        self.input_data = []
        for i in range(event_count):
            self.input_data.append(
                {
                    "data": f"payload_{alias}_{i:05}",
                }
            )

        self.expected_output = [{"data": row["data"], "event_time": create_time} for row in self.input_data]


class MigrationTableInfo:
    def __init__(self, alias, event_timestamp, path):
        self.alias = alias
        self.event_timestamp = event_timestamp
        self.path = path
        self.input_data = [{"data": f"{alias}_{index:03}"} for index in range(MIGRATION_EVENT_COUNT)]
        self.expected_output = [{"data": row["data"], "event_time": event_timestamp} for row in self.input_data]


@yt.wrapper.yt_dataclass
class InnerScope:
    data: str


@yt.wrapper.yt_dataclass
class CompositeStruct:
    data: InnerScope


@yt.wrapper.yt_dataclass
class StrictOptionalRow:
    data: typing.Optional[InnerScope] = None


class StrictCompositeTableInfo:
    def __init__(self, alias, create_time, event_count, input_dir):
        self.alias = alias
        self.create_time = create_time
        self.path = input_dir + "/" + datetime.datetime.utcfromtimestamp(create_time).isoformat()
        self.input_data = []
        for i in range(event_count):
            self.input_data.append(
                CompositeStruct(
                    data=InnerScope(
                        data=f"payload_{alias}_{i:05}",
                    ),
                )
            )
        self.expected_output = [{"data": row.data.data, "event_time": create_time} for row in self.input_data]


class WeakCompositeTableInfo:
    def __init__(self, alias, create_time, event_count, input_dir):
        self.alias = alias
        self.create_time = create_time
        self.path = input_dir + "/" + datetime.datetime.utcfromtimestamp(create_time).isoformat()
        self.input_data = []
        for i in range(event_count):
            self.input_data.append(
                {
                    "data": {
                        "data": f"payload_{alias}_{i:05}",
                    }
                }
            )
        self.expected_output = [{"data": row["data"]["data"], "event_time": create_time} for row in self.input_data]


class StrictOptionalTableInfo:
    def __init__(self, alias, create_time, event_count, input_dir, null_pattern="non_null_first"):
        self.alias = alias
        self.create_time = create_time
        self.path = input_dir + "/" + datetime.datetime.utcfromtimestamp(create_time).isoformat()
        self.input_data = []
        for i in range(event_count):
            is_null = {
                "non_null_first": i % 2 != 0,
                "null_first": i % 2 == 0,
                "all_null": True,
            }[null_pattern]
            self.input_data.append(
                StrictOptionalRow(
                    data=None if is_null else InnerScope(data=f"payload_{alias}_{i:05}"),
                )
            )
        self.expected_output = [
            {"data": row.data.data, "event_time": create_time}
            for row in self.input_data
            if row.data is not None
        ]


class StrictYsonTableInfo:
    # YSON values written as String into a V1 any column
    SCHEMA = [{"name": "data", "type": "any"}]

    def __init__(self, alias, create_time, event_count, input_dir):
        self.alias = alias
        self.create_time = create_time
        self.path = input_dir + "/" + datetime.datetime.utcfromtimestamp(create_time).isoformat()
        self.input_data = []
        for i in range(event_count):
            self.input_data.append(
                {
                    "data": yt.yson.dumps(
                        {
                            "data": f"payload_{alias}_{i:05}",
                        },
                    ),
                }
            )
        self.expected_output = [{"data": yt.yson.loads(row["data"]).get("data"), "event_time": create_time} for row in self.input_data]


class WeakOptionalTableInfo:
    def __init__(self, alias, create_time, event_count, input_dir, null_pattern="non_null_first"):
        self.alias = alias
        self.create_time = create_time
        self.path = input_dir + "/" + datetime.datetime.utcfromtimestamp(create_time).isoformat()
        self.input_data = []
        for i in range(event_count):
            is_null = {
                "non_null_first": i % 2 != 0,
                "null_first": i % 2 == 0,
                "all_null": True,
            }[null_pattern]
            self.input_data.append({
                "data": None if is_null else f"payload_{alias}_{i:05}",
            })
        self.expected_output = [
            {"data": row["data"], "event_time": create_time}
            for row in self.input_data
            if row["data"] is not None
        ]

##################################################################


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def setup_method(self, method):
        super(Test, self).setup_method(method)
        self.input_dir = self.work_yt_path + "/input"
        self.client.create("map_node", self.input_dir)
        self.first_input_table = TableInfo("first", int(1.5e9), EVENT_COUNT, self.input_dir)
        self.second_input_table = TableInfo("second", int(1.6e9), EVENT_COUNT, self.input_dir)
        self.strict_composite_input_table = StrictCompositeTableInfo("strict_composite", int(1.5e9), EVENT_COUNT, self.input_dir)
        self.weak_composite_input_table = WeakCompositeTableInfo("weak_composite", int(1.5e9), EVENT_COUNT, self.input_dir)
        self.output_queue = self.work_yt_path + "/output_queue"

    def get_output(self):
        return sorted(self.client.select_rows(f"data, event_time from [{self.output_queue}]"), key=lambda x: x["data"])

    def prepare_input_table(self, input_table):
        self.prepare_input_table_on(self.client, input_table)

    def prepare_strict_composite_input_table(self, input_table):
        try:
            self.client.write_table_structured(input_table.path, CompositeStruct, input_table.input_data)
        except Exception as e:
            raise Exception(f"Failed to prepare strict composite input table {input_table.path}") from e

    def prepare_strict_optional_input_table(self, input_table):
        try:
            self.client.write_table_structured(input_table.path, StrictOptionalRow, input_table.input_data)
        except Exception as e:
            raise Exception(f"Failed to prepare strict optional input table {input_table.path}") from e

    def prepare_strict_yson_input_table(self, input_table):
        with self.client.Transaction():
            try:
                self.client.create("table", input_table.path, attributes={"schema": input_table.SCHEMA})
                self.client.write_table(input_table.path, input_table.input_data)
            except Exception as e:
                raise Exception(f"Failed to prepare strict yson input table {input_table.path}") from e

    def prepare_input_table_on(self, client, input_table):
        with client.Transaction():
            try:
                client.create("table", input_table.path)
                client.write_table(input_table.path, input_table.input_data)
            except Exception as e:
                raise Exception(f"Failed to prepare input table {input_table.path}") from e

    def prepare_pipeline_config(
        self,
        pipeline_type: str,
        process_two_tables: bool = False,
        process_directory: bool = False,
        finite: bool = True,
        desired_table_process_time: datetime.timedelta = datetime.timedelta(seconds=1),
        add_bad_source: bool = False,
        clusters: list[str] | None = None,
        source_class_name: str | None = None,
        use_migration_timestamps: bool = False,
    ):
        config_path, sink_computation = {
            "swift": (PIPELINE_SWIFT_CONFIG_PATH, "reader"),
            "transform": (PIPELINE_TRANSFORM_CONFIG_PATH, "writer"),
        }[pipeline_type]
        pipeline_config = get_yson_config(config_path)

        source_parameters = pipeline_config["spec"]["computations"]["reader"]["source_streams"]["table"]["parameters"]
        source_spec = pipeline_config["spec"]["computations"]["reader"]["source_streams"]["table"]
        if source_class_name is not None:
            source_spec["source_class_name"] = source_class_name
        source_parameters["finite"] = finite
        if use_migration_timestamps:
            source_parameters.update(
                {
                    "event_timestamp_locator": {"attribute": "event_timestamp", "format": "iso8601"},
                    "system_timestamp_locator": {"attribute": "system_timestamp", "format": "iso8601"},
                }
            )
        if clusters is not None:
            clusters_str = ";".join(clusters)
            source_parameters["tables_path"] = f"<clusters=[{clusters_str}]>{self.input_dir}"
        elif process_directory:
            source_parameters["tables_path"] = f"<cluster=primary>{self.input_dir}"
        else:
            tables = [f"<cluster=primary>{self.first_input_table.path}"]
            if process_two_tables:
                tables.append(f"<cluster=primary>{self.second_input_table.path}")
            source_parameters["tables"] = tables

        pipeline_config["spec"]["computations"][sink_computation]["sinks"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.output_queue}",
            }
        )

        pipeline_config["dynamic_spec"]["computations"]["reader"]["source_streams"]["table"]["parameters"].update(
            {
                "desired_table_process_time": desired_table_process_time.total_seconds() * 1000,
            }
        )

        reader_empty_spec = pipeline_config["spec"]["computations"].get("reader_empty")
        if reader_empty_spec:
            empty_source_parameters = reader_empty_spec["source_streams"]["table"]["parameters"]
            empty_source_parameters["finite"] = finite
            if add_bad_source:
                assert not process_two_tables and pipeline_type == "swift"
                empty_source_parameters["tables"] = [f"<cluster=primary>{self.first_input_table.path}_bad"]

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def prepare_migration_tables(self, include_t3=False):
        event_t1 = int(1.5e9)
        event_t2 = int(1.6e9)
        event_t3 = int(1.7e9)

        def prepare(table, system_timestamp):
            timestamp_attributes = {
                "event_timestamp": datetime.datetime.fromtimestamp(table.event_timestamp, datetime.UTC).isoformat(),
                "system_timestamp": datetime.datetime.fromtimestamp(system_timestamp, datetime.UTC).isoformat(),
            }
            self.client.create("table", table.path, attributes=timestamp_attributes)
            self.client.write_table(table.path, table.input_data)

        first = MigrationTableInfo("object_first", event_t1, self.input_dir + "/temporary_first")
        second = MigrationTableInfo("object_second", event_t1, self.input_dir + "/temporary_second")
        prepare(first, event_t1)
        prepare(second, event_t1)

        initial_by_object_id = sorted([first, second], key=lambda table: self.client.get(table.path + "/@id"))
        for table, name in zip(initial_by_object_id, ["z_t1", "a_t1"]):
            new_path = self.input_dir + "/" + name
            self.client.move(table.path, new_path)
            table.path = new_path

        by_object_id = sorted([first, second], key=lambda table: self.client.get(table.path + "/@id"))
        by_name = sorted([first, second], key=lambda table: table.path.rsplit("/", 1)[-1])

        t2 = MigrationTableInfo("t2", event_t2, self.input_dir + "/m_t2")
        prepare(t2, event_t2)
        t3 = None
        if include_t3:
            t3 = MigrationTableInfo("t3", event_t3, self.input_dir + "/n_t3")
            prepare(t3, event_t3)

        return {
            "t1_v1": by_object_id,
            "t1_v2": by_name,
            "t2": t2,
            "t3": t3,
        }

    def prepare_migration_pipeline_config(self):
        return self.prepare_pipeline_config(
            pipeline_type="swift",
            process_directory=True,
            finite=False,
            desired_table_process_time=datetime.timedelta(minutes=5),
            source_class_name=V1_SOURCE_CLASS,
            use_migration_timestamps=True,
        )

    def get_ordered_output(self):
        return list(
            self.client.select_rows(
                f"data, event_time FROM [{self.output_queue}] " "ORDER BY [$tablet_index], [$row_index] LIMIT 1000000"
            )
        )

    def set_source_class(self, source_class_name):
        spec = self.client.get_pipeline_spec(self.pipeline_path)
        spec["spec"]["computations"]["reader"]["source_streams"]["table"]["source_class_name"] = source_class_name
        self.client.set_pipeline_spec(
            self.pipeline_path,
            spec["spec"],
            expected_version=spec["version"],
        )

    def update_source_dynamic_parameters(self, **updates):
        dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
        parameters = dynamic_spec["spec"]["computations"]["reader"]["source_streams"]["table"]["parameters"]
        for name, value in updates.items():
            if value is None:
                parameters.pop(name, None)
            else:
                parameters[name] = value
        self.client.set_pipeline_dynamic_spec(
            self.pipeline_path,
            dynamic_spec["spec"],
            expected_version=dynamic_spec["version"],
        )

    def get_source_controller_state(self):
        computations = self.client.get_flow_view(
            self.pipeline_path,
            view_path="/state/job_manager_state/computations",
            cache=False,
        )
        state = computations["reader"]["/sources/table/v0"]
        if isinstance(state, (bytes, str)):
            return yt.yson.loads(state)
        return state

    @staticmethod
    def get_migration_mode(state):
        mode = state["mode"]
        return (mode.decode() if isinstance(mode, bytes) else str(mode)).lower()

    def wait_source_state(self, predicate):
        result = {}

        def check():
            state = self.get_source_controller_state()
            if not predicate(state):
                return False
            result["state"] = state
            return True

        wait(check, timeout=180, ignore_exceptions=True)
        return result["state"]

    def wait_source_mode(self, expected_mode):
        return self.wait_source_state(lambda state: self.get_migration_mode(state) == expected_mode)

    def stop_pipeline(self):
        self.client.stop_pipeline(self.pipeline_path)
        self.wait_pipeline_state(["stopped", "completed"], timeout=180)

    def stage_committed_draining(self, tables):
        t1_expected = sum((table.expected_output for table in tables["t1_v1"]), [])
        full_expected = t1_expected + tables["t2"].expected_output
        if tables["t3"] is not None:
            full_expected += tables["t3"].expected_output

        wait(
            lambda: 0 < len(self.get_ordered_output()) < MIGRATION_EVENT_COUNT,
            timeout=180,
        )
        self.stop_pipeline()
        stopped_output = self.get_ordered_output()
        assert stopped_output == full_expected[: len(stopped_output)]
        assert len(stopped_output) < MIGRATION_EVENT_COUNT

        self.set_source_class(V2_SOURCE_CLASS)
        self.update_source_dynamic_parameters(
            allow_v1_migration=False,
            desired_table_process_time="2m",
        )
        self.client.start_pipeline(self.pipeline_path)
        self.wait_pipeline_state("working", timeout=180)

        self.wait_source_mode("v1")
        wait(
            lambda: len(stopped_output) < len(self.get_ordered_output()) < MIGRATION_EVENT_COUNT,
            timeout=180,
        )
        v1_output = self.get_ordered_output()
        assert v1_output == full_expected[: len(v1_output)]

        self.update_source_dynamic_parameters(allow_v1_migration=True)
        draining = self.wait_source_mode("draining")

        self.stop_pipeline()
        self.set_source_class(V1_SOURCE_CLASS)
        self.update_source_dynamic_parameters(allow_v1_migration=False)
        self.client.start_pipeline(self.pipeline_path)
        self.wait_pipeline_state("working", timeout=180)
        reloaded = self.wait_source_mode("draining")
        assert reloaded["cutover_era"] == draining["cutover_era"]
        assert reloaded["cutover_event_timestamp"] == draining["cutover_event_timestamp"]
        return full_expected, draining

    @pytest.mark.authors(["pechatnov"])
    def test_v1_to_v2_migration(self):
        run_yt_sync("primary", self.work_yt_path)
        tables = self.prepare_migration_tables()
        pipeline_config_path = self.prepare_migration_pipeline_config()

        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            full_expected, _ = self.stage_committed_draining(tables)
            self.update_source_dynamic_parameters(desired_table_process_time="1s")
            self.wait_source_mode("v2")
            wait(lambda: len(self.get_ordered_output()) == len(full_expected), timeout=180)
            output = self.get_ordered_output()
            assert output == full_expected
            for table in tables["t1_v1"]:
                for row in table.expected_output:
                    assert sum(item["data"] == row["data"] for item in output) == 1

    @pytest.mark.authors(["pechatnov"])
    def test_v2_disabled_rolls_back_to_v1(self):
        run_yt_sync("primary", self.work_yt_path)
        tables = self.prepare_migration_tables()
        full_expected = sum((table.expected_output for table in tables["t1_v1"]), []) + tables["t2"].expected_output
        pipeline_config_path = self.prepare_migration_pipeline_config()

        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            self.wait_source_mode("v1")
            wait(
                lambda: 0 < len(self.get_ordered_output()) < MIGRATION_EVENT_COUNT,
                timeout=180,
            )
            self.stop_pipeline()
            stopped_output = self.get_ordered_output()
            assert stopped_output == full_expected[: len(stopped_output)]

            self.set_source_class(V2_SOURCE_CLASS)
            self.update_source_dynamic_parameters(
                desired_table_process_time="2m",
            )
            self.client.start_pipeline(self.pipeline_path)
            self.wait_pipeline_state("working", timeout=180)
            self.wait_source_mode("v1")
            wait(
                lambda: len(stopped_output) < len(self.get_ordered_output()) < 2 * MIGRATION_EVENT_COUNT,
                timeout=180,
            )
            self.stop_pipeline()

            self.set_source_class(V1_SOURCE_CLASS)
            self.update_source_dynamic_parameters(
                desired_table_process_time="1s",
            )
            self.client.start_pipeline(self.pipeline_path)
            self.wait_pipeline_state("working", timeout=180)
            self.wait_source_mode("v1")
            wait(lambda: len(self.get_ordered_output()) == len(full_expected), timeout=180)
            assert self.get_ordered_output() == full_expected

    @pytest.mark.authors(["pechatnov"])
    def test_restart_instant_replays_before_visible_greater_table(self):
        run_yt_sync("primary", self.work_yt_path)
        tables = self.prepare_migration_tables(include_t3=True)
        deferred_input_dir = self.work_yt_path + "/deferred_input"
        self.client.create("map_node", deferred_input_dir)
        for table in [tables["t2"], tables["t3"]]:
            deferred_path = deferred_input_dir + "/" + table.path.rsplit("/", 1)[-1]
            self.client.move(table.path, deferred_path)
            table.path = deferred_path
        pipeline_config_path = self.prepare_migration_pipeline_config()

        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            self.stage_committed_draining(tables)
            self.update_source_dynamic_parameters(desired_table_process_time="1s")
            self.wait_source_mode("v2")
            self.update_source_dynamic_parameters(desired_table_process_time="5m")
            for table in [tables["t2"], tables["t3"]]:
                visible_path = self.input_dir + "/" + table.path.rsplit("/", 1)[-1]
                self.client.move(table.path, visible_path)
                table.path = visible_path
            wait(
                lambda: 2 * MIGRATION_EVENT_COUNT < len(self.get_ordered_output()) < 3 * MIGRATION_EVENT_COUNT,
                timeout=180,
            )
            before_restart = self.get_source_controller_state()
            assert before_restart["distributing_table"]["event_timestamp"] == tables["t2"].event_timestamp
            old_era = before_restart["era"]

            self.update_source_dynamic_parameters(restart_instant=datetime.datetime.now(datetime.UTC).isoformat())
            restarted = self.wait_source_state(lambda state: state["era"] > old_era)
            new_era = restarted["era"]
            self.wait_source_state(
                lambda state: state["distributing_table"]["era"] == new_era
                and state["distributing_table"]["event_timestamp"] == tables["t1_v1"][0].event_timestamp
            )
            self.update_source_dynamic_parameters(desired_table_process_time="5s")
            self.wait_source_state(
                lambda state: state["distributing_table"]["era"] == new_era
                and state["distributing_table"]["event_timestamp"] == tables["t2"].event_timestamp
            )

            output = self.get_ordered_output()
            for table in tables["t1_v1"]:
                for row in table.expected_output:
                    assert sum(item["data"] == row["data"] for item in output) == 2
            assert not any(item["data"].startswith("t3_") for item in output)

            self.update_source_dynamic_parameters(desired_table_process_time="1s")
            wait(
                lambda: sum(item["data"].startswith("t3_") for item in self.get_ordered_output())
                == MIGRATION_EVENT_COUNT,
                timeout=180,
            )

    @pytest.mark.authors(["pechatnov"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count", "problems", "pipeline_type"),
        [
            pytest.param(
                1,
                1,
                False,
                "swift",
                id="swift_1c_1w_stable",
            ),
            pytest.param(4, 2, True, "swift", id="swift_2c_4w_unstable"),
            pytest.param(4, 2, True, "transform", id="transform_2c_4w_unstable"),
        ],
    )
    def test_one_input_table(self, workers_count, controllers_count, problems, pipeline_type):
        run_yt_sync("primary", self.work_yt_path)
        self.prepare_input_table(self.first_input_table)
        pipeline_config_path = self.prepare_pipeline_config(pipeline_type=pipeline_type)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=workers_count,
            controllers_count=controllers_count,
            problems=problems,
        ):
            self.wait_pipeline_state("completed", timeout=180)
            assert self.get_output() == self.first_input_table.expected_output
            assert len(list(self.client.select_rows(f"* FROM [{self.pipeline_path}/states] LIMIT 10000"))) == 0

            def check_partitions_cleaned():
                rows = list(self.client.select_rows(
                    f"* FROM [{self.pipeline_path}/flow_state] "
                    'WHERE state_name = "layout_partitions" AND value IS NOT NULL '
                    "LIMIT 10000"
                ))
                return len(rows) == 0

            # Partitions of source computation must be cleaned and partitions of transform must not.
            if pipeline_type != "transform":
                wait(check_partitions_cleaned, timeout=180)

    @pytest.mark.authors(["pechatnov"])
    def test_two_input_tables(self):
        run_yt_sync("primary", self.work_yt_path)
        self.prepare_input_table(self.first_input_table)
        self.prepare_input_table(self.second_input_table)
        pipeline_config_path = self.prepare_pipeline_config(pipeline_type="swift", process_two_tables=True)
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            self.wait_pipeline_state("completed", timeout=180)
            assert self.get_output() == self.first_input_table.expected_output + self.second_input_table.expected_output

    @pytest.mark.authors(["pechatnov"])
    def test_table_directory(self):
        run_yt_sync("primary", self.work_yt_path)
        self.prepare_input_table(self.first_input_table)
        pipeline_config_path = self.prepare_pipeline_config(pipeline_type="swift", process_directory=True, finite=False)
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            wait(lambda: len(self.get_output()) == EVENT_COUNT, timeout=180)

            logging.info("First table is read")

            self.prepare_input_table(self.second_input_table)
            wait(lambda: len(self.get_output()) == EVENT_COUNT * 2, timeout=180)

            assert self.get_output() == self.first_input_table.expected_output + self.second_input_table.expected_output

    @pytest.mark.authors(["pechatnov"])
    def test_throttling(self):
        run_yt_sync("primary", self.work_yt_path)
        self.prepare_input_table(self.first_input_table)
        pipeline_config_path = self.prepare_pipeline_config(
            pipeline_type="swift", desired_table_process_time=datetime.timedelta(minutes=5)
        )
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            wait(lambda: len(self.get_output()) > 0, timeout=180)

            assert self.client.get_pipeline_state(self.pipeline_path) != "completed"

            dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
            dynamic_spec["spec"]["computations"]["reader"]["source_streams"]["table"]["parameters"][
                "desired_table_process_time"
            ] = "1s"
            self.client.set_pipeline_dynamic_spec(
                self.pipeline_path, dynamic_spec["spec"], expected_version=dynamic_spec["version"]
            )

            self.wait_pipeline_state("completed", timeout=180)

            assert self.get_output() == self.first_input_table.expected_output

    @pytest.mark.authors(["pechatnov"])
    def test_removing_table(self):
        run_yt_sync("primary", self.work_yt_path)
        self.prepare_input_table(self.first_input_table)
        pipeline_config_path = self.prepare_pipeline_config(
            pipeline_type="swift", process_directory=True, desired_table_process_time=datetime.timedelta(minutes=5)
        )
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}) as federation:
            wait(lambda: len(self.get_output()) > 0, timeout=180)

            self.client.remove(self.first_input_table.path)

            federation.workers[0].restart()  # Now partition can not be completed.

            assert self.client.get_pipeline_state(self.pipeline_path) != "completed"

            dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
            dynamic_spec["spec"]["computations"]["reader"]["source_streams"]["table"]["parameters"][
                "desired_table_process_time"
            ] = "1s"
            self.client.set_pipeline_dynamic_spec(
                self.pipeline_path, dynamic_spec["spec"], expected_version=dynamic_spec["version"]
            )

            self.wait_pipeline_state("completed", timeout=180)

            assert len(self.get_output()) != EVENT_COUNT
            assert len(list(self.client.select_rows(f"* FROM [{self.pipeline_path}/states] LIMIT 10000"))) == 0

    @pytest.mark.authors(["pechatnov"])
    def test_restart_table(self):
        run_yt_sync("primary", self.work_yt_path)
        self.prepare_input_table(self.first_input_table)
        pipeline_config_path = self.prepare_pipeline_config(
            pipeline_type="swift", process_directory=True, desired_table_process_time=datetime.timedelta(minutes=5)
        )
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            wait(lambda: len(self.get_output()) > 0, timeout=180)

            assert self.client.get_pipeline_state(self.pipeline_path) != "completed"

            dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
            dynamic_spec["spec"]["computations"]["reader"]["source_streams"]["table"]["parameters"].update(
                {
                    "desired_table_process_time": "5s",
                    "restart_instant": datetime.datetime.now(datetime.UTC).isoformat(),
                }
            )
            self.client.set_pipeline_dynamic_spec(
                self.pipeline_path, dynamic_spec["spec"], expected_version=dynamic_spec["version"]
            )

            self.wait_pipeline_state("completed", timeout=40)

            assert len(self.get_output()) > EVENT_COUNT
            assert len(list(self.client.select_rows(f"* FROM [{self.pipeline_path}/states] LIMIT 10000"))) == 0

    @pytest.mark.authors(["pechatnov"])
    def test_extra_bad_source(self):
        run_yt_sync("primary", self.work_yt_path)
        self.prepare_input_table(self.first_input_table)
        pipeline_config_path = self.prepare_pipeline_config(pipeline_type="swift", add_bad_source=True)
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            wait(lambda: self.get_output() == self.first_input_table.expected_output, timeout=180)
            assert self.client.get_pipeline_state(self.pipeline_path) != "completed"

    @pytest.mark.authors(["mosgor"])
    def test_strict_composite_table_rows(self):
        run_yt_sync("primary", self.work_yt_path)
        self.prepare_strict_composite_input_table(self.strict_composite_input_table)
        pipeline_config_path = self.prepare_pipeline_config(pipeline_type="swift")
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            self.wait_pipeline_state("completed", timeout=180)
            assert self.get_output() == self.strict_composite_input_table.expected_output

    @pytest.mark.authors(["mosgor"])
    def test_weak_composite_table_rows(self):
        run_yt_sync("primary", self.work_yt_path)
        self.prepare_input_table(self.weak_composite_input_table)
        pipeline_config_path = self.prepare_pipeline_config(pipeline_type="swift")
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            self.wait_pipeline_state("completed", timeout=180)
            assert self.get_output() == self.weak_composite_input_table.expected_output

    @pytest.mark.authors(["mosgor"])
    @pytest.mark.parametrize(
        "null_pattern",
        [
            "non_null_first",
            "null_first",
            "all_null",
        ],
    )
    def test_strict_optional_column(self, null_pattern):
        run_yt_sync("primary", self.work_yt_path)
        table_info = StrictOptionalTableInfo("strict_optional", int(1.5e9), EVENT_COUNT, self.input_dir, null_pattern)
        self.prepare_strict_optional_input_table(table_info)
        self.first_input_table = table_info
        pipeline_config_path = self.prepare_pipeline_config(pipeline_type="swift")
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            self.wait_pipeline_state("completed", timeout=180)
            assert self.get_output() == table_info.expected_output

    @pytest.mark.authors(["mosgor"])
    @pytest.mark.parametrize(
        "null_pattern",
        [
            "non_null_first",
            "null_first",
            "all_null",
        ],
    )
    def test_weak_optional_column(self, null_pattern):
        run_yt_sync("primary", self.work_yt_path)
        table_info = WeakOptionalTableInfo("weak_optional", int(1.5e9), EVENT_COUNT, self.input_dir, null_pattern)
        self.prepare_input_table(table_info)
        self.first_input_table = table_info
        pipeline_config_path = self.prepare_pipeline_config(pipeline_type="swift")
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            self.wait_pipeline_state("completed", timeout=180)
            assert self.get_output() == table_info.expected_output

    @pytest.mark.authors(["mosgor"])
    def test_strict_yson_table(self):
        run_yt_sync("primary", self.work_yt_path)
        table_info = StrictYsonTableInfo("strict_yson", int(1.5e9), EVENT_COUNT, self.input_dir)
        self.prepare_strict_yson_input_table(table_info)
        self.first_input_table = table_info
        pipeline_config_path = self.prepare_pipeline_config(pipeline_type="swift")
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            self.wait_pipeline_state("completed", timeout=180)
            assert self.get_output() == table_info.expected_output

    @pytest.mark.authors(["htual"])
    def test_multi_cluster_reads_replica_once(self):
        # The same directory holds an identical replica of the table on two clusters. The source
        # (TablesPath with clusters=[...]) must read each logical table exactly once: the per-cluster
        # listings are merged and deduplicated by name, so the output never doubles.
        replica_cluster = self.remote_cluster_names[0]
        replica_client = self.cluster_name_to_client[replica_cluster]

        run_yt_sync("primary", self.work_yt_path)
        replica_client.create("map_node", self.input_dir)
        self.prepare_input_table_on(self.client, self.first_input_table)
        self.prepare_input_table_on(replica_client, self.first_input_table)

        pipeline_config_path = self.prepare_pipeline_config(
            pipeline_type="swift",
            clusters=["primary", replica_cluster],
            finite=True,
        )
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            self.wait_pipeline_state("completed", timeout=180)
            # Exactly once: no missing, corrupted, extra or duplicated rows.
            assert self.get_output() == self.first_input_table.expected_output
            assert len(list(self.client.select_rows(f"* FROM [{self.pipeline_path}/states] LIMIT 10000"))) == 0

    @pytest.mark.authors(["htual"])
    def test_multi_cluster_reads_tables_from_both_clusters(self):
        # Two distinct tables live on different clusters (neither replicated). The multi-cluster source
        # must read the union — each table once — in event-timestamp order.
        replica_cluster = self.remote_cluster_names[0]
        replica_client = self.cluster_name_to_client[replica_cluster]

        run_yt_sync("primary", self.work_yt_path)
        replica_client.create("map_node", self.input_dir)
        self.prepare_input_table_on(self.client, self.first_input_table)      # earlier event timestamp
        self.prepare_input_table_on(replica_client, self.second_input_table)  # later event timestamp

        pipeline_config_path = self.prepare_pipeline_config(
            pipeline_type="swift",
            clusters=["primary", replica_cluster],
            finite=True,
        )
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            self.wait_pipeline_state("completed", timeout=180)
            assert self.get_output() == self.first_input_table.expected_output + self.second_input_table.expected_output
            assert len(list(self.client.select_rows(f"* FROM [{self.pipeline_path}/states] LIMIT 10000"))) == 0

    @pytest.mark.authors(["htual"])
    def test_multi_cluster_failover(self):
        # The active cluster loses the table mid-read; an identical replica is available on the other
        # cluster only. The source must fail over to it, deliver every row, and reclaim the ranges it
        # abandoned on the active cluster (no leaked source key state — asserted via empty /states).
        replica_cluster = self.remote_cluster_names[0]
        replica_client = self.cluster_name_to_client[replica_cluster]

        run_yt_sync("primary", self.work_yt_path)
        replica_client.create("map_node", self.input_dir)
        # Initially only the primary cluster (first listed, hence active) holds the table.
        self.prepare_input_table_on(self.client, self.first_input_table)

        pipeline_config_path = self.prepare_pipeline_config(
            pipeline_type="swift",
            clusters=["primary", replica_cluster],
            finite=True,
            # Throttle so the table is only partially processed before we trigger failover.
            desired_table_process_time=datetime.timedelta(minutes=5),
        )
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            wait(lambda: len(self.get_output()) > 0, timeout=180)
            assert len(self.get_output()) < EVENT_COUNT

            # Replica appears before we drop the active copy, so the table is never absent everywhere.
            self.prepare_input_table_on(replica_client, self.first_input_table)
            self.client.remove(self.first_input_table.path)

            # Speed reading up so the failed-over table finishes and the pipeline can complete.
            dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
            dynamic_spec["spec"]["computations"]["reader"]["source_streams"]["table"]["parameters"][
                "desired_table_process_time"
            ] = "1s"
            self.client.set_pipeline_dynamic_spec(
                self.pipeline_path, dynamic_spec["spec"], expected_version=dynamic_spec["version"]
            )

            self.wait_pipeline_state("completed", timeout=180)
            # Failover re-reads the tail, so duplicates are allowed, but every row must be present.
            expected_data = {row["data"] for row in self.first_input_table.expected_output}
            assert {row["data"] for row in self.get_output()} == expected_data
            assert len(self.get_output()) >= EVENT_COUNT
            assert len(list(self.client.select_rows(f"* FROM [{self.pipeline_path}/states] LIMIT 10000"))) == 0

    @pytest.mark.authors(["htual"])
    def test_multi_cluster_rereads_recreated_table_on_active_cluster(self):
        # On the active cluster the table is deleted and re-created under the same name but with
        # different data (a new object id). The source must detect the recreation, reread from scratch,
        # deliver the new data, and reclaim the abandoned ranges of the old object (empty /states).
        replica_cluster = self.remote_cluster_names[0]
        replica_client = self.cluster_name_to_client[replica_cluster]

        run_yt_sync("primary", self.work_yt_path)
        replica_client.create("map_node", self.input_dir)

        # Same create_time → same directory name/path; different alias → different payload.
        original = TableInfo("original", int(1.5e9), EVENT_COUNT, self.input_dir)
        recreated = TableInfo("recreated", int(1.5e9), EVENT_COUNT, self.input_dir)
        assert original.path == recreated.path
        self.prepare_input_table_on(self.client, original)

        pipeline_config_path = self.prepare_pipeline_config(
            pipeline_type="swift",
            clusters=["primary", replica_cluster],
            finite=True,
            desired_table_process_time=datetime.timedelta(minutes=5),
        )
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            wait(lambda: len(self.get_output()) > 0, timeout=180)
            assert len(self.get_output()) < EVENT_COUNT

            # Delete → create under the same name yields a new object id (physically a different table).
            self.client.remove(original.path)
            self.prepare_input_table_on(self.client, recreated)

            dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
            dynamic_spec["spec"]["computations"]["reader"]["source_streams"]["table"]["parameters"][
                "desired_table_process_time"
            ] = "1s"
            self.client.set_pipeline_dynamic_spec(
                self.pipeline_path, dynamic_spec["spec"], expected_version=dynamic_spec["version"]
            )

            self.wait_pipeline_state("completed", timeout=180)
            # Every row of the recreated table must be delivered (full reread of the new object).
            output_data = {row["data"] for row in self.get_output()}
            assert {row["data"] for row in recreated.expected_output} <= output_data
            assert len(list(self.client.select_rows(f"* FROM [{self.pipeline_path}/states] LIMIT 10000"))) == 0
