"""End-to-end test for key-visitor streams in TSwiftMapComputation: messages
from a finite queue source store their payload into external state, visits
mutate that state only (bump `visit_count`, no output streams at all). After
`completed` every seeded key must keep its payload and carry at least one
visit — the guaranteed final pass runs after the inputs are done."""

import logging

import pytest
import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from .yt_sync import run_yt_sync

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline_swift/pipeline.yson")


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline_swift/pipeline")

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.input_consumer = self.work_yt_path + "/consumer"
        self.state = self.work_yt_path + "/state"

    def send_keys(self, entries):
        rows = [{"key": k, "payload": p} for k, p in entries]
        self.client.insert_rows(self.input_queue, rows)

    def prepare_pipeline_config(self, period_ms=20000, finite=True):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        source_params = pipeline_config["spec"]["computations"]["key_reader"]["source_streams"]["queue"]["parameters"]
        source_params.update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.input_consumer}",
                "finite": finite,
            }
        )

        pipeline_config["spec"]["computations"]["tester"]["external_state_managers"]["/state"]["parameters"][
            "path"
        ] = self.state

        pipeline_config["dynamic_spec"]["computations"]["tester"]["key_visitor_streams"]["visit_iter"][
            "period"
        ] = period_ms

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["mikari"])
    def test_swift_key_visitor(self):
        run_yt_sync("primary", self.work_yt_path, with_swift_state=True)

        seeded = [(f"k_{i:03d}", f"v_{i}") for i in range(20)]
        expected_payloads = dict(seeded)

        pipeline_config_path = self.prepare_pipeline_config(period_ms=20000, finite=True)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.send_keys(seeded)
            self.wait_pipeline_state("completed", timeout=240)

            rows = list(self.client.select_rows(f"`key`, `payload`, `visit_count` from [{self.state}]"))
            actual = {row["key"]: row for row in rows}

            missing = set(expected_payloads) - set(actual)
            assert not missing, (
                f"pipeline reached `completed` but {len(missing)} seeded keys are absent from the state table: "
                f"{sorted(missing)[:10]}{'...' if len(missing) > 10 else ''}"
            )
            for key, expected_payload in expected_payloads.items():
                row = actual[key]
                assert (
                    row["payload"] == expected_payload
                ), f"key={key!r}: state payload is {row['payload']!r}, expected {expected_payload!r}"
                assert row["visit_count"] >= 1, f"key={key!r} was never visited (visit_count={row['visit_count']})"
            logging.info("swift key_visitor passed (rows=%d)", len(actual))
