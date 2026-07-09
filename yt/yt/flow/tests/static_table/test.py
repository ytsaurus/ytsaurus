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
        with self.client.Transaction():
            try:
                self.client.create("table", input_table.path)
                self.client.write_table(input_table.path, input_table.input_data)
            except Exception as e:
                raise Exception(f"Failed to prepare input table {input_table.path}") from e

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

    def prepare_pipeline_config(
        self,
        pipeline_type: str,
        process_two_tables: bool = False,
        process_directory: bool = False,
        finite: bool = True,
        desired_table_process_time: datetime.timedelta = datetime.timedelta(seconds=1),
        add_bad_source: bool = False,
    ):
        config_path, sink_computation = {
            "swift": (PIPELINE_SWIFT_CONFIG_PATH, "reader"),
            "transform": (PIPELINE_TRANSFORM_CONFIG_PATH, "writer"),
        }[pipeline_type]
        pipeline_config = get_yson_config(config_path)

        source_parameters = pipeline_config["spec"]["computations"]["reader"]["source_streams"]["table"]["parameters"]
        source_parameters["finite"] = finite
        if process_directory:
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
