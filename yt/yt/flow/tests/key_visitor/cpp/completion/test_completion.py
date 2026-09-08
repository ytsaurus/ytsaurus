"""When a key-visitor stream finalizes: standalone visitors with `finite` on, off and flipped, and
`upstream_streams` narrowing the wait."""

import logging

import pytest

from yt.common import wait, WaitFailed

from yt.yt.flow.tests.key_visitor.cpp.common.base import KeyVisitorTestBase, pipeline_binary


class Test(KeyVisitorTestBase):
    def _reviser_partition_ids(self):
        return list(self.partition_states("reviser_like"))

    def _reviser_partition_states(self):
        return self.partition_states("reviser_like")

    # A computation whose only work-source is key_visitor_streams (no inputs, no
    # sources) must be accepted, range-partitioned into the requested count, and
    # produce a visit per key seeded directly into the external state. Standalone,
    # so the partitions keep sweeping for the whole test instead of retiring after
    # their single pass.
    @pytest.mark.authors(["blinkov"])
    def test_key_visitor_only_accepted_and_partitioned(self):
        self.run_yt_sync(with_external_state=True)

        entries = [(f"k_{i:03d}", f"v_{i}") for i in range(20)]
        expected_keys = {k for k, _ in entries}

        pipeline_config_path = self.prepare_pipeline_keyvisitor_only_config(
            period_ms=1000, desired_partition_count=4, finite=False
        )
        with self.start_flow_process_federation(
            binary_path=pipeline_binary("pipeline_keyvisitor_only"),
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.wait_pipeline_state("working", timeout=120)
            self.seed_user_state(entries)

            wait(lambda: len(self._reviser_partition_ids()) == 4, timeout=120, ignore_exceptions=True)
            assert len(self._reviser_partition_ids()) == 4

            def visited_keys():
                return {row["key"] for row in self.get_output() if row["visit_index"] >= 1}

            wait(lambda: visited_keys() >= expected_keys, timeout=120, ignore_exceptions=True)
            missing = expected_keys - visited_keys()
            assert not missing, f"key-visitor-only pipeline never visited {len(missing)} keys: {sorted(missing)[:10]}"
            logging.info("cpp key_visitor-only passed (keys=%d)", len(expected_keys))

    # finite = %false makes the visitor a standalone periodic scanner: it never arms
    # a final pass, so it keeps sweeping and its partitions stay Executing indefinitely. The
    # sibling test above returns within the first pass and so cannot see this.
    @pytest.mark.authors(["sergeypozdeev"])
    def test_key_visitor_standalone_never_self_completes(self):
        self.run_yt_sync(with_external_state=True)

        entries = [(f"k_{i:03d}", f"v_{i}") for i in range(10)]
        expected_keys = {k for k, _ in entries}

        pipeline_config_path = self.prepare_pipeline_keyvisitor_only_config(
            period_ms=1000, desired_partition_count=2, finite=False
        )
        with self.start_flow_process_federation(
            binary_path=pipeline_binary("pipeline_keyvisitor_only"),
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.wait_pipeline_state("working", timeout=120)
            self.seed_user_state(entries)

            # visit_index counts visits per key in the external state, so three visits of every
            # key mean the visitor rotated its pass at least twice — one rotation past the point
            # where the old code marked a pass final.
            def keys_swept(times):
                return {key for key, index in self.max_visit_index_per_key().items() if index >= times}

            try:
                wait(lambda: keys_swept(3) >= expected_keys, timeout=60, ignore_exceptions=True)
            except WaitFailed as ex:
                raise AssertionError(
                    f"visitor stopped sweeping: keys below three visits "
                    f"{sorted(expected_keys - keys_swept(3))[:10]}, "
                    f"partition states {self._reviser_partition_states()}"
                ) from ex

            # Belt and braces: with the visitor stalled the wait above fires first, so these
            # only cover a partition retiring while sweeping still looks healthy.
            states = self._reviser_partition_states()
            retired = {pid: state for pid, state in states.items() if state in ("completing", "completed")}
            assert not retired, f"standalone key-visitor partitions retired themselves: {states}"
            assert "executing" in states.values(), f"no partition left running: {states}"

    # The default, finite = %true, on a computation with no upstream at all: initialization
    # marks the very first pass Final before scanning starts, so every key is swept once and
    # the partitions then retire. The single sweep is the point: the pass is seeded Final,
    # rather than becoming Final only on the rotation after it.
    @pytest.mark.authors(["sergeypozdeev"])
    def test_key_visitor_only_finite_completes_after_one_pass(self):
        self.run_yt_sync(with_external_state=True)

        entries = [(f"k_{i:03d}", f"v_{i}") for i in range(10)]
        expected_keys = {k for k, _ in entries}

        # Seeded before the pipeline starts: there is exactly one sweep, so a range the
        # visitor covers before the rows land would never be revisited.
        self.seed_user_state(entries)

        pipeline_config_path = self.prepare_pipeline_keyvisitor_only_config(
            period_ms=1000, desired_partition_count=2, finite=True
        )
        with self.start_flow_process_federation(
            binary_path=pipeline_binary("pipeline_keyvisitor_only"),
            pipeline_binary_args={"--config": pipeline_config_path},
        ):

            def all_partitions_retired():
                states = self._reviser_partition_states()
                return bool(states) and set(states.values()) == {"completed"}

            wait(all_partitions_retired, timeout=120, ignore_exceptions=True)
            assert all_partitions_retired(), f"partitions did not retire: {self._reviser_partition_states()}"

            visits = self.max_visit_index_per_key()
            assert set(visits) == expected_keys, f"keys missed by the final pass: {expected_keys - set(visits)}"
            swept_twice = {key: index for key, index in visits.items() if index > 1}
            assert not swept_twice, f"keys swept more than once before retiring: {swept_twice}"

    # A standalone visitor has no end of input of its own, so the test says when it should
    # stop: flipping `finite` on the running pipeline makes the visitor finish its sweep and
    # retire. That is a real end-of-work barrier, which stopping the pipeline would not be —
    # a drain does not wait for unread input.
    @pytest.mark.authors(["sergeypozdeev"])
    def test_key_visitor_standalone_completes_after_flip(self):
        self.run_yt_sync(with_external_state=True)

        entries = [(f"k_{i:03d}", f"v_{i}") for i in range(10)]
        expected_keys = {k for k, _ in entries}

        pipeline_config_path = self.prepare_pipeline_keyvisitor_only_config(
            period_ms=1000, desired_partition_count=2, finite=False
        )
        with self.start_flow_process_federation(
            binary_path=pipeline_binary("pipeline_keyvisitor_only"),
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.wait_pipeline_state("working", timeout=120)
            self.seed_user_state(entries)

            # The work the test came for, observed while the visitor is still unbounded.
            wait(
                lambda: set(self.max_visit_index_per_key()) >= expected_keys,
                timeout=60,
                ignore_exceptions=True,
            )
            assert set(self.max_visit_index_per_key()) >= expected_keys

            self.ask_key_visitor_to_complete("reviser_like", "visit_iter")
            self.wait_pipeline_state("completed", timeout=120)

            states = self._reviser_partition_states()
            assert set(states.values()) == {"completed"}, f"partitions did not retire: {states}"

    # `upstream_streams` narrows what the visitor waits for. `tester` reads a finite
    # `finite_keys` stream, and every visit it emits goes out as a ping request that comes
    # back on `ping_responses` — a stream that cannot end before the visitor does. Told to
    # follow `finite_keys` alone, the visitor runs its final pass once the input drains, the
    # loop then drains behind it and the whole pipeline reaches `completed`.
    @pytest.mark.authors(["vv-glazkov"])
    def test_key_visitor_upstream_streams_narrow_the_wait(self):
        self.run_yt_sync()

        # Both batches are queued before the pipeline starts, so the finite source cannot
        # complete before it has delivered v2 — and the visitor cannot finalize before that
        # either, since it follows that source's stream.
        v1 = [(f"k_{i:03d}", f"v1_{i}") for i in range(10)]
        v2 = [(f"k_{i:03d}", f"v2_{i}") for i in range(10)]

        pipeline_config_path = self.prepare_pipeline_visitor_loop_config(
            period_ms=1000, upstream_streams=["finite_keys"]
        )
        self.send_keys(v1)
        self.send_keys(v2)
        with self.start_flow_process_federation(
            binary_path=pipeline_binary("pipeline_visitor_loop"),
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.wait_pipeline_state("completed", timeout=120)

            # Only a pass that ran after the input was processed sees v2, so an earlier
            # finalization would leave v1 here.
            self.assert_latest_payloads(dict(v2))

    # The same pipeline with the default wait: the visitor also waits for `ping_responses`,
    # which is fed by its own visits, so neither can ever finish and it sweeps for good.
    @pytest.mark.authors(["vv-glazkov"])
    def test_key_visitor_without_upstream_streams_waits_for_every_input(self):
        self.run_yt_sync()

        entries = [(f"k_{i:03d}", f"v_{i}") for i in range(10)]
        expected_keys = {k for k, _ in entries}

        pipeline_config_path = self.prepare_pipeline_visitor_loop_config(period_ms=1000, upstream_streams=None)
        self.send_keys(entries)
        with self.start_flow_process_federation(
            binary_path=pipeline_binary("pipeline_visitor_loop"),
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.wait_pipeline_state("working", timeout=120)

            # The finite input is fully read: a narrowed visitor would be retiring by now.
            wait(
                lambda: bool(self.partition_states("key_reader"))
                and set(self.partition_states("key_reader").values()) == {"completed"},
                timeout=120,
                ignore_exceptions=True,
            )
            assert set(self.partition_states("key_reader").values()) == {
                "completed"
            }, f"finite input never retired: {self.partition_states('key_reader')}"

            # Three more visits of every key mean the visitor rotated its pass at least twice
            # past the point where it would have stopped had it not waited for the loop.
            wait(
                lambda: set(self.max_visit_index_per_key()) >= expected_keys,
                timeout=120,
                ignore_exceptions=True,
            )
            visited = self.max_visit_index_per_key()

            def keys_swept_again(times):
                return {
                    key for key, index in self.max_visit_index_per_key().items() if index >= visited.get(key, 0) + times
                }

            try:
                wait(lambda: keys_swept_again(3) >= expected_keys, timeout=120, ignore_exceptions=True)
            except WaitFailed as ex:
                raise AssertionError(
                    "visitor stopped although the loop it feeds never completed: keys below three further "
                    f"visits {sorted(expected_keys - keys_swept_again(3))[:10]}"
                ) from ex
