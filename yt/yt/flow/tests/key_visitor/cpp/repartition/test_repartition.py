"""`key_visitor_states` bookkeeping under repartitioning: an interrupted partition must not conflict
with its overlapping successors."""

import logging
import time

import pytest

from yt.common import wait

from yt.yt.flow.tests.key_visitor.cpp.common.base import KeyVisitorTestBase, pipeline_binary


class Test(KeyVisitorTestBase):
    def _set_tester_partition_count(self, count):
        dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
        tester = dynamic_spec["spec"]["computations"]["tester"]
        tester.setdefault("parameters", {})["desired_partition_count"] = count
        self.client.set_pipeline_dynamic_spec(
            self.pipeline_path, dynamic_spec["spec"], expected_version=dynamic_spec["version"]
        )

    def _tester_partition_ids(self):
        return list(self.partition_states("tester"))

    def _key_visitor_row_count(self):
        rows = list(self.client.select_rows(f"sum(1) as cnt from [{self.pipeline_path}/key_visitor_states] group by 1"))
        return rows[0]["cnt"] if rows else 0

    def _row_lock_conflict_seen(self):
        # A row lock conflict surfaces in the flow view as a job failure
        # (ephemeral_state/previous_job_fail_error or current_job_status/error).
        # Require both the 1700 message and the table name so unrelated transient
        # errors don't trip it.
        view = self.client.get_flow_view(self.pipeline_path, cache=False)
        blob = str(view)
        return "Row lock conflict due to concurrent write" in blob and "key_visitor_states" in blob

    # Repartition a key-visitor computation repeatedly under load and assert it
    # never produces a key_visitor_states row lock conflict: the interrupted
    # partition must not erase coverage that overlapping successors are writing.
    @pytest.mark.authors(["sergeypozdeev"])
    def test_no_key_visitor_states_conflict_on_repartition(self):
        self.run_yt_sync()

        # Infinite source + short visitor period so successors sweep aggressively
        # during the interrupt window; start at a single partition so each
        # repartition interrupts a partition whose range overlaps every successor.
        pipeline_config_path = self.prepare_pipeline_config(period_ms=500, finite=False, desired_partition_count=1)
        with self.start_flow_process_federation(
            binary_path=pipeline_binary("pipeline"),
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.wait_pipeline_state("working", timeout=120)

            # Substantial key state => the cleanup erase touches many coverage rows.
            keys = [(f"k_{i:04d}", f"v_{i}") for i in range(500)]
            self.send_keys(keys)
            wait(lambda: self._key_visitor_row_count() > 0, timeout=120, ignore_exceptions=True)

            # Each change interrupts the current partitions and creates a new
            # overlapping set; poll for the conflict while the old ones drain.
            for count in [8, 2, 8, 3]:
                old_ids = set(self._tester_partition_ids())
                self._set_tester_partition_count(count)
                wait(
                    lambda: len(set(self._tester_partition_ids()) - old_ids) > 0,
                    timeout=90,
                    sleep_backoff=0.1,
                    ignore_exceptions=True,
                )
                deadline = time.time() + 15
                while time.time() < deadline:
                    assert (
                        not self._row_lock_conflict_seen()
                    ), f"key_visitor_states row lock conflict during repartition (desired_partition_count={count})"
                    self.send_keys(keys)
                    time.sleep(1)

            assert (
                not self._row_lock_conflict_seen()
            ), "key_visitor_states row lock conflict observed after repartitions"
            logging.info("cpp key_visitor repartition test passed without row lock conflicts")
