import logging
import time

import pytest
import yatest.common

from yt.common import wait
from yt.wrapper import yson

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from .yt_sync import run_yt_sync

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")

SOURCE_UNSORTED_SCHEMA = [
    {"name": "hash", "type": "uint64", "expression": "farm_hash(key)"},
    {"name": "key", "type": "string"},
    {"name": "payload", "type": "string"},
]

SOURCE_SORTED_SCHEMA = [
    {"name": "hash", "type": "uint64", "sort_order": "ascending"},
    {"name": "key", "type": "string", "sort_order": "ascending"},
    {"name": "payload", "type": "string"},
]


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.input_consumer = self.work_yt_path + "/consumer"
        self.source = self.work_yt_path + "/source"
        self.source_unsorted = self.work_yt_path + "/source_unsorted"
        self.source_data_prefix = self.work_yt_path + "/source_data_"
        self.source_version = 0

    def set_source(self, keys):
        self.source_version += 1
        data_path = f"{self.source_data_prefix}{self.source_version}"

        self.client.remove(self.source_unsorted, force=True)
        self.client.create("table", self.source_unsorted, attributes={"schema": SOURCE_UNSORTED_SCHEMA})
        self.client.write_table(self.source_unsorted, [{"key": k, "payload": f"p_{k}"} for k in keys])
        self.client.run_sort(
            self.source_unsorted,
            self.client.TablePath(data_path, attributes={"schema": yson.to_yson_type(SOURCE_SORTED_SCHEMA)}),
            sort_by=["hash", "key"],
        )

        self.client.link(data_path, self.source, force=True)

    def get_mirror_keys(self):
        res = self.client.read_states(
            self.pipeline_path,
            computation_id="mirror",
            target="key_state",
            limit=1000,
        )
        keys = set()
        for row in res["key_states"]:
            state = row["states"].get("/mirror")
            if state is None:
                continue
            payload = state.get("payload", "")
            assert payload.startswith("p_"), f"unexpected mirror payload {payload!r}"
            keys.add(payload[len("p_") :])
        return keys

    def prepare_pipeline_config(self, period_ms=1000, unavailable_source_policy="retry"):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        source_params = pipeline_config["spec"]["computations"]["key_reader"]["source_streams"]["queue"]["parameters"]
        source_params.update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.input_consumer}",
                "finite": False,
            }
        )

        joiner_params = pipeline_config["spec"]["computations"]["mirror"]["external_state_joiners"]["/source"][
            "parameters"
        ]
        joiner_params["path"] = self.source
        joiner_params["unavailable_source_policy"] = unavailable_source_policy

        pipeline_config["dynamic_spec"]["computations"]["mirror"]["key_visitor_streams"]["visit_iter"][
            "period"
        ] = period_ms
        pipeline_config["dynamic_spec"]["computations"]["mirror"]["external_state_joiners"] = {
            "/source": {"parameters": {"unavailable_source_backoff": 5000}}
        }

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["mikari"])
    def test_mirror_converges_to_source(self):
        run_yt_sync("primary", self.work_yt_path)

        pipeline_config_path = self.prepare_pipeline_config(period_ms=1000)
        initial = {f"k_{i:03d}" for i in range(20)}
        self.set_source(initial)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            wait(lambda: self.get_mirror_keys() == initial, timeout=240)
            logging.info("scenario 1 converged: mirror == source (%d keys)", len(initial))

            phase_a = {"a", "b", "c"}
            self.set_source(phase_a)
            wait(lambda: self.get_mirror_keys() == phase_a, timeout=240)
            logging.info("scenario 2 phase A converged: mirror == {a,b,c}")

            phase_b = {"b", "c", "d"}
            self.set_source(phase_b)
            wait(lambda: self.get_mirror_keys() == phase_b, timeout=240)
            mirror = self.get_mirror_keys()
            assert "a" not in mirror, "key 'a' should have been deleted from the mirror"
            assert "d" in mirror, "key 'd' should have been added to the mirror"
            assert mirror == phase_b
            logging.info("scenario 2 phase B converged: mirror == {b,c,d} (a deleted, d added)")

    @pytest.mark.authors(["mikari"])
    def test_unreadable_source_does_not_delete_mirror(self):
        run_yt_sync("primary", self.work_yt_path)

        pipeline_config_path = self.prepare_pipeline_config(period_ms=1000, unavailable_source_policy="mark_unreadable")
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            seeded = {"a", "b", "c"}
            self.set_source(seeded)
            wait(lambda: self.get_mirror_keys() == seeded, timeout=240)
            logging.info("seeded mirror converged: mirror == {a,b,c}")

            self.client.remove(self.source)

            for _ in range(20):
                time.sleep(1)
                assert self.get_mirror_keys() == seeded, "unreadable source must not delete the mirror"
            logging.info("mirror preserved across 20 sweeps with the source removed: mirror == {a,b,c}")

            recovered = {"a", "b", "c", "e"}
            self.set_source(recovered)
            wait(lambda: self.get_mirror_keys() == recovered, timeout=240)
            logging.info("mirror recovered after recreate: mirror == {a,b,c,e}")
