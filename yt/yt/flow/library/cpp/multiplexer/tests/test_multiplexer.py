import logging
import time

import pytest

import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from yt.common import wait

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")


##################################################################


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def setup_method(self, method):
        super(Test, self).setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.input_consumer = self.work_yt_path + "/consumer"
        self.output_queue = self.work_yt_path + "/output_queue"
        self.secondary_index = self.work_yt_path + "/secondary_index"

    def get_output(self):
        return list(self.client.select_rows(f"`key`, `secondary_key`, region, payload from [{self.output_queue}]"))

    def get_output_for_key(self, key):
        return [row for row in self.get_output() if row["key"] == key]

    def send_key(self, key, payload):
        self.client.insert_rows(self.input_queue, [{"key": key, "payload": payload}])

    def send_keys(self, entries):
        """entries: list of (key, payload)."""
        rows = [{"key": k, "payload": p} for k, p in entries]
        self.client.insert_rows(self.input_queue, rows)

    def seed_secondary_index(self, rows_per_key):
        """rows_per_key: dict key -> total_rows. Inserts rows with secondary_key in [0, total_rows)."""
        self.client.mount_table(self.secondary_index)
        rows = []
        for key, count in rows_per_key.items():
            for i in range(count):
                rows.append({
                    "key": key,
                    "secondary_key": i,
                    "region": f"region-{key}-{i}",
                })
        self.client.insert_rows(self.secondary_index, rows)

    def prepare_pipeline_config(self, batch_size=50, timer_period=2000):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        source_params = pipeline_config["spec"]["computations"]["key_reader"]["source_streams"]["queue"]["parameters"]
        source_params.update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.input_consumer}",
                "finite": False,
            }
        )

        multiplexer_params = pipeline_config["spec"]["computations"]["multiplexer"]["parameters"]
        multiplexer_params["table_path"] = f"<cluster=primary>{self.secondary_index}"

        sink_params = pipeline_config["spec"]["computations"]["multiplexer"]["sinks"]["queue"]["parameters"]
        sink_params.update(
            {
                "queue_path": f"<cluster=primary>{self.output_queue}",
            }
        )

        pipeline_config["dynamic_spec"]["computations"]["multiplexer"]["parameters"].update(
            {
                "batch_size": batch_size,
                "timer_period": timer_period,
            }
        )

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def assert_exact_output_for_payload(self, key, expected_payload, expected_total_rows):
        """Verify output for (key, payload) has the full set of secondary_keys with correct regions."""
        output = self.get_output_for_key(key)
        matching = [r for r in output if r["payload"] == expected_payload]
        actual_secondary_keys = sorted(r["secondary_key"] for r in matching)
        expected_secondary_keys = list(range(expected_total_rows))
        assert actual_secondary_keys == expected_secondary_keys, (
            f"key={key!r}, payload={expected_payload!r}: "
            f"expected secondary_keys 0..{expected_total_rows - 1} ({expected_total_rows} rows), "
            f"got {len(matching)} rows, "
            f"missing={sorted(set(expected_secondary_keys) - set(actual_secondary_keys))[:5]}, "
            f"extra={sorted(set(actual_secondary_keys) - set(expected_secondary_keys))[:5]}"
        )
        for row in matching:
            expected_region = f"region-{key}-{row['secondary_key']}"
            assert row["region"] == expected_region, (
                f"region mismatch for key={key!r}, secondary_key={row['secondary_key']}: "
                f"expected {expected_region!r}, got {row['region']!r}"
            )

    # ------------------------------------------------------------------
    # Test 1: Basic — multiple keys with different row counts (not divisible by batch_size=50).
    # ------------------------------------------------------------------
    @pytest.mark.authors(["mikari"])
    def test_basic_multiple_keys(self):
        """Multiple keys, each backed by N rows in the lookup table. All rows delivered with correct payload."""
        run_yt_sync("primary", self.work_yt_path)

        rows_per_key = {"alpha": 149, "beta": 130, "gamma": 77}
        self.seed_secondary_index(rows_per_key)

        pipeline_config_path = self.prepare_pipeline_config(batch_size=50)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.send_keys([(k, "P1") for k in rows_per_key])

            expected_total = sum(rows_per_key.values())
            wait(lambda: len(self.get_output()) >= expected_total, timeout=180)

            for key, total in rows_per_key.items():
                self.assert_exact_output_for_payload(key, "P1", total)

            logging.info("test_basic_multiple_keys passed")

    # ------------------------------------------------------------------
    # Test 2: Single collapse — send key with payload=P1, wait for partial progress,
    # re-send with payload=P2. Verify exactly N rows with payload=P2.
    # ------------------------------------------------------------------
    @pytest.mark.authors(["mikari"])
    def test_collapse_single(self):
        """Collapse: re-send key with new payload mid-iteration. Final output: exact rows with new payload."""
        run_yt_sync("primary", self.work_yt_path)

        rows_per_key = {"collapse_key": 149}
        self.seed_secondary_index(rows_per_key)

        pipeline_config_path = self.prepare_pipeline_config(batch_size=50)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.send_key("collapse_key", "P1")

            wait(lambda: len(self.get_output_for_key("collapse_key")) > 0, timeout=120)
            logging.info("Rows before collapse: %d", len(self.get_output_for_key("collapse_key")))

            # Collapse: re-send with payload=P2.
            self.send_key("collapse_key", "P2")

            # Wait for a complete set with payload=P2.
            wait(
                lambda: len([r for r in self.get_output_for_key("collapse_key") if r["payload"] == "P2"]) >= 149,
                timeout=180,
            )

            self.assert_exact_output_for_payload("collapse_key", "P2", 149)
            logging.info(
                "test_collapse_single passed: total rows = %d", len(self.get_output_for_key("collapse_key"))
            )

    # ------------------------------------------------------------------
    # Test 3: Multi-collapse — send key 3 times with payloads P1, P2, P3.
    # ------------------------------------------------------------------
    @pytest.mark.authors(["mikari"])
    def test_multi_collapse(self):
        """Triple collapse. Final pass produces exactly N rows with the last payload."""
        run_yt_sync("primary", self.work_yt_path)

        rows_per_key = {"multi": 130}
        self.seed_secondary_index(rows_per_key)

        pipeline_config_path = self.prepare_pipeline_config(batch_size=30)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.send_key("multi", "P1")

            wait(lambda: len(self.get_output_for_key("multi")) > 0, timeout=120)
            logging.info("After payload=P1: %d rows", len(self.get_output_for_key("multi")))

            # Collapse #1.
            self.send_key("multi", "P2")

            time.sleep(3)

            # Collapse #2.
            self.send_key("multi", "P3")

            # Wait for a complete set with payload=P3.
            wait(
                lambda: len([r for r in self.get_output_for_key("multi") if r["payload"] == "P3"]) >= 130,
                timeout=180,
            )

            self.assert_exact_output_for_payload("multi", "P3", 130)
            logging.info("test_multi_collapse passed: total rows = %d", len(self.get_output_for_key("multi")))
