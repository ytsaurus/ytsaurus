"""The final pass of a key-visitor stream in TSwiftMapComputation: messages from a finite queue
source store their payload into external state, visits mutate that state only (bump `visit_count`,
no output streams at all). After `completed` every seeded key must keep its payload and carry at
least one visit — the guaranteed final pass runs after the inputs are done."""

import logging

import pytest

from yt.yt.flow.tests.key_visitor.cpp.common.base import KeyVisitorTestBase, pipeline_binary


class Test(KeyVisitorTestBase):
    @pytest.mark.authors(["mikari"])
    def test_swift_key_visitor(self):
        self.run_yt_sync(with_swift_state=True)

        seeded = [(f"k_{i:03d}", f"v_{i}") for i in range(20)]
        expected_payloads = dict(seeded)

        pipeline_config_path = self.prepare_pipeline_swift_config(period_ms=5000, finite=True)
        self.send_keys(seeded)
        with self.start_flow_process_federation(
            binary_path=pipeline_binary("pipeline_swift"),
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.wait_pipeline_state("completed", timeout=240)

            rows = list(self.client.select_rows(f"`key`, `payload`, `visit_count` from [{self.swift_state}]"))
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
