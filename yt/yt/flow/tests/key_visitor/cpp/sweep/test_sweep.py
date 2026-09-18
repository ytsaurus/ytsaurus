"""The final pass of a key-visitor stream: v1 then v2 for the same keys from a finite source; after
`completed` the latest visit of every key must carry v2. One test per kind of swept state."""

import logging

import pytest

from yt.yt.flow.tests.key_visitor.cpp.common.base import KeyVisitorTestBase, pipeline_binary

# v2 overwrites every key of v1 before the pipeline starts, so an early final pass would report v1.
V1 = [(f"k_{i:03d}", f"v1_{i}") for i in range(20)]
V2 = [(f"k_{i:03d}", f"v2_{i}") for i in range(20)]
EXPECTED_LATEST = dict(V2)


class Test(KeyVisitorTestBase):
    @pytest.mark.authors(["mikari"])
    def test_internal_state(self):
        self.run_yt_sync()

        pipeline_config_path = self.prepare_pipeline_config(period_ms=5000, finite=True)
        self.send_keys(V1)
        self.send_keys(V2)
        with self.start_flow_process_federation(
            binary_path=pipeline_binary("pipeline"),
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.wait_pipeline_state("completed", timeout=240)
            self.assert_latest_payloads(EXPECTED_LATEST)
            logging.info("cpp key_visitor passed (rows=%d)", len(EXPECTED_LATEST))

    # manual_preload: the function preloads every message and visit key of the batch itself.
    @pytest.mark.authors(["mikari", "sergeypozdeev"])
    @pytest.mark.parametrize(
        "config_name",
        [
            pytest.param("pipeline.yson", id="auto_preload"),
            pytest.param("pipeline_manual.yson", id="manual_preload"),
        ],
    )
    def test_external_state(self, config_name):
        self.run_yt_sync(with_external_state=True)

        pipeline_config_path = self.prepare_pipeline_external_config(
            period_ms=5000, finite=True, config_name=config_name
        )
        self.send_keys(V1)
        self.send_keys(V2)
        with self.start_flow_process_federation(
            binary_path=pipeline_binary("pipeline_external"),
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.wait_pipeline_state("completed", timeout=240)
            self.assert_latest_payloads(EXPECTED_LATEST, what="seeded keys (via external state)")
            logging.info("cpp key_visitor external-state sweep passed (rows=%d)", len(EXPECTED_LATEST))
