"""End-to-end test for the python KeyVisitor companion: same v1/v2 ingest,
wait `completed`, assert the latest visit row per key carries v2 — proves the
final pass scanned post-completion state and routed visits to ``on_visit``."""

import logging

import pytest
import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_python_base import FlowTestPythonBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from .yt_sync import run_yt_sync


PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")


class Test(FlowTestPythonBase):
    PYTHON_COMPANION_BINARY = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.input_consumer = self.work_yt_path + "/consumer"
        self.output_queue = self.work_yt_path + "/output_queue"

    def get_output(self):
        return list(self.client.select_rows(f"`key`, `payload`, `visit_index` from [{self.output_queue}]"))

    def send_keys(self, entries):
        rows = [{"key": k, "payload": p} for k, p in entries]
        self.client.insert_rows(self.input_queue, rows)

    def prepare_pipeline_config(self, period_ms=20000, finite=True):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        source_params = pipeline_config["spec"]["computations"]["key_reader"]["source_streams"]["queue"]["parameters"]
        source_params.update({
            "queue_path": f"<cluster=primary>{self.input_queue}",
            "consumer_path": f"<cluster=primary>{self.input_consumer}",
            "finite": finite,
        })

        sink_params = pipeline_config["spec"]["computations"]["tester"]["sinks"]["queue"]["parameters"]
        sink_params.update({"queue_path": f"<cluster=primary>{self.output_queue}"})

        pipeline_config["dynamic_spec"]["computations"]["tester"]["key_visitor_streams"]["visit_iter"]["period"] = period_ms

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["mikari"])
    def test_key_visitor(self):
        run_yt_sync("primary", self.work_yt_path)

        v1 = [(f"k_{i:03d}", f"v1_{i}") for i in range(20)]
        v2 = [(f"k_{i:03d}", f"v2_{i}") for i in range(20)]
        expected_keys = {k for k, _ in v1}
        expected_latest = {k: p for k, p in v2}

        pipeline_config_path = self.prepare_pipeline_config(period_ms=20000, finite=True)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.send_keys(v1)
            self.send_keys(v2)
            self.wait_pipeline_state("completed", timeout=240)

            latest = {}
            for row in self.get_output():
                idx = row["visit_index"]
                if idx > latest.get(row["key"], (-1, None))[0]:
                    latest[row["key"]] = (idx, row["payload"])

            missing = expected_keys - set(latest)
            assert not missing, (
                f"pipeline reached `completed` but {len(missing)} seeded keys were never visited: "
                f"{sorted(missing)[:10]}{'...' if len(missing) > 10 else ''}"
            )
            for key, expected_payload in expected_latest.items():
                actual_payload = latest[key][1]
                assert actual_payload == expected_payload, (
                    f"key={key!r}: latest visit had payload {actual_payload!r}, expected {expected_payload!r}"
                )
            logging.info("python key_visitor passed (rows=%d)", sum(1 for _ in latest))
