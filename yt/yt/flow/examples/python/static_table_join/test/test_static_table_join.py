"""E2E test for the Python static-table-join companion."""

import datetime

import pytest
import yatest.common

from yt.common import wait
from yt.yt.flow.library.python.integration_test_base.yt_flow_python_base import FlowTestPythonBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(
    "yt/yt/flow/examples/python/static_table_join/pipeline.yson"
)

# Reference rows whose names need trimming + lowercasing before they are usable
# as join attributes. V2 simulates a periodic batch rebuild of the reference
# table carrying a new value for the same key.
REFERENCE_ROW = {"key": 1, "name": "  Alice "}
NORMALIZED_NAME = "alice"
REFERENCE_ROW_V2 = {"key": 1, "name": "  Bob "}
NORMALIZED_NAME_V2 = "bob"

# The reference stream is uint64-keyed, so the snapshot tables must declare a
# matching schema (an unschematized table would infer int64 for the key).
REFERENCE_SCHEMA = [
    {"name": "key", "type": "uint64"},
    {"name": "name", "type": "string"},
]

#################################################################


class TestStaticTableJoin(FlowTestPythonBase):
    PYTHON_COMPANION_BINARY = yatest.common.binary_path(
        "yt/yt/flow/examples/python/static_table_join/static_table_join"
    )

    def setup_method(self, method):
        super(TestStaticTableJoin, self).setup_method(method)
        self.reference_dir = self.work_yt_path + "/reference"
        # The static_table connector derives a table's event time from its name,
        # so each periodically-rebuilt snapshot is named with an ISO8601 stamp.
        snapshot_time = datetime.datetime.fromtimestamp(int(1.5e9), datetime.timezone.utc)
        rebuild_time = snapshot_time + datetime.timedelta(hours=1)
        self.reference_table = self.reference_dir + "/" + snapshot_time.strftime("%Y-%m-%dT%H:%M:%S")
        self.reference_table_v2 = self.reference_dir + "/" + rebuild_time.strftime("%Y-%m-%dT%H:%M:%S")
        self.reference_state = self.work_yt_path + "/reference_state"
        self.event_queue = self.work_yt_path + "/event_queue"
        self.event_consumer = self.work_yt_path + "/event_consumer"
        self.output_queue = self.work_yt_path + "/output_queue"
        self.output_producer = self.work_yt_path + "/output_producer"

    def prepare_environment(self):
        run_yt_sync("primary", self.work_yt_path)

        self.client.create("map_node", self.reference_dir)
        with self.client.Transaction():
            self.client.create("table", self.reference_table, attributes={"schema": REFERENCE_SCHEMA})
            self.client.write_table(self.reference_table, [REFERENCE_ROW])

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)
        computations = pipeline_config["spec"]["computations"]

        # Watch the whole directory: a new ISO8601-named snapshot appearing in it
        # must automatically trigger a new run.
        computations["reference_reader"]["source_streams"]["reference_table"]["parameters"].update(
            {
                "tables_path": f"<cluster=primary>{self.reference_dir}",
                "finite": False,
            }
        )
        computations["event_reader"]["source_streams"]["event_queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.event_queue}",
                "consumer_path": f"<cluster=primary>{self.event_consumer}",
                "finite": False,
            }
        )
        computations["reference_loader"]["external_state_managers"]["/reference_state"]["parameters"][
            "path"
        ] = f"<cluster=primary>{self.reference_state}"
        computations["enricher"]["external_state_joiners"]["/reference_state"]["parameters"][
            "path"
        ] = f"<cluster=primary>{self.reference_state}"
        computations["enricher"]["sinks"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.output_queue}",
                "producer_path": f"<cluster=primary>{self.output_producer}",
            }
        )

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def get_output(self):
        return list(self.client.select_rows(f"key, name from [{self.output_queue}]"))

    @pytest.mark.authors(["blinkov"])
    def test_join(self):
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config()
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
        ):
            # Wait until the reference row has been transformed and persisted to
            # the shared state table by the loader.
            wait(lambda: len(list(self.client.select_rows(f"* from [{self.reference_state}]"))) == 1, timeout=180)

            self.client.insert_rows(self.event_queue, [{"key": 1}])

            wait(lambda: self.get_output() == [{"key": 1, "name": NORMALIZED_NAME}], timeout=180)

            # A periodic batch rebuild drops a new snapshot into the watched
            # directory; the loader must fully reload the state from it.
            with self.client.Transaction():
                self.client.create("table", self.reference_table_v2, attributes={"schema": REFERENCE_SCHEMA})
                self.client.write_table(self.reference_table_v2, [REFERENCE_ROW_V2])

            wait(
                lambda: [row["normalized_name"] for row in self.client.select_rows(f"* from [{self.reference_state}]")]
                == [NORMALIZED_NAME_V2],
                timeout=180,
            )

            # The SAME key joined again must see the rebuilt reference: the
            # enricher re-reads the state instead of serving the value it
            # already joined once.
            self.client.insert_rows(self.event_queue, [{"key": 1}])

            wait(
                lambda: self.get_output()
                == [{"key": 1, "name": NORMALIZED_NAME}, {"key": 1, "name": NORMALIZED_NAME_V2}],
                timeout=180,
            )
