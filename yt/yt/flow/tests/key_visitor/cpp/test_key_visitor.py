"""End-to-end test for the cpp KeyVisitor: send v1 then v2 for the same keys
from a finite queue source, wait for `completed`, and assert the latest visit
row for every key carries the v2 payload — proves the final pass scanned
post-completion state."""

import logging
import time

import pytest
import yatest.common

from yt.common import wait, WaitFailed

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from .yt_sync import run_yt_sync

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")
PIPELINE_EXTERNAL_CONFIG_PATH = yatest.common.source_path(
    f"{yatest.common.context.project_path}/pipeline_external/pipeline.yson"
)
PIPELINE_KEYVISITOR_ONLY_CONFIG_PATH = yatest.common.source_path(
    f"{yatest.common.context.project_path}/pipeline_keyvisitor_only/pipeline.yson"
)


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")
    FLOW_EXTERNAL_BINARY_PATH = yatest.common.binary_path(
        f"{yatest.common.context.project_path}/pipeline_external/pipeline"
    )
    FLOW_KEYVISITOR_ONLY_BINARY_PATH = yatest.common.binary_path(
        f"{yatest.common.context.project_path}/pipeline_keyvisitor_only/pipeline"
    )

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.input_consumer = self.work_yt_path + "/consumer"
        self.output_queue = self.work_yt_path + "/output_queue"
        self.user_state = self.work_yt_path + "/user_state"

    def get_output(self):
        return list(self.client.select_rows(f"`key`, `payload`, `visit_index` from [{self.output_queue}]"))

    def send_keys(self, entries):
        rows = [{"key": k, "payload": p} for k, p in entries]
        self.client.insert_rows(self.input_queue, rows)

    def prepare_pipeline_config(self, period_ms=20000, finite=True, desired_partition_count=None):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        source_params = pipeline_config["spec"]["computations"]["key_reader"]["source_streams"]["queue"]["parameters"]
        source_params.update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.input_consumer}",
                "finite": finite,
            }
        )

        sink_params = pipeline_config["spec"]["computations"]["tester"]["sinks"]["queue"]["parameters"]
        sink_params.update({"queue_path": f"<cluster=primary>{self.output_queue}"})

        tester_dynamic = pipeline_config["dynamic_spec"]["computations"]["tester"]
        tester_dynamic["key_visitor_streams"]["visit_iter"]["period"] = period_ms
        if desired_partition_count is not None:
            tester_dynamic.setdefault("parameters", {})["desired_partition_count"] = desired_partition_count

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def prepare_pipeline_external_config(self, period_ms=1000, finite=True):
        pipeline_config = get_yson_config(PIPELINE_EXTERNAL_CONFIG_PATH)

        source_params = pipeline_config["spec"]["computations"]["key_reader"]["source_streams"]["queue"]["parameters"]
        source_params.update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.input_consumer}",
                "finite": finite,
            }
        )

        sink_params = pipeline_config["spec"]["computations"]["tester"]["sinks"]["queue"]["parameters"]
        sink_params.update(
            {
                "queue_path": f"<cluster=primary>{self.output_queue}",
            }
        )

        pipeline_config["spec"]["computations"]["tester"]["external_state_managers"]["/user-state-external"][
            "parameters"
        ]["path"] = self.user_state

        pipeline_config["dynamic_spec"]["computations"]["tester"]["key_visitor_streams"]["visit_iter"][
            "period"
        ] = period_ms

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline_external.yson")

    def prepare_pipeline_keyvisitor_only_config(self, period_ms=1000, desired_partition_count=4, finite=True):
        pipeline_config = get_yson_config(PIPELINE_KEYVISITOR_ONLY_CONFIG_PATH)

        computation = pipeline_config["spec"]["computations"]["reviser_like"]
        computation["external_state_managers"]["/user-state"]["parameters"]["path"] = self.user_state
        computation["sinks"]["queue"]["parameters"].update({"queue_path": f"<cluster=primary>{self.output_queue}"})

        dynamic = pipeline_config["dynamic_spec"]["computations"]["reviser_like"]
        dynamic["key_visitor_streams"]["visit_iter"]["period"] = period_ms
        dynamic["key_visitor_streams"]["visit_iter"]["finite"] = finite
        dynamic.setdefault("parameters", {})["desired_partition_count"] = desired_partition_count

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline_keyvisitor_only.yson")

    def seed_user_state(self, entries):
        rows = [{"key": k, "payload": p, "visit_index": 0} for k, p in entries]
        self.client.insert_rows(self.user_state, rows)

    def _reviser_partition_ids(self):
        partitions = self.client.get_flow_view(
            self.pipeline_path, view_path="/state/execution_spec/layout/partitions", cache=False
        )
        return [pid for pid, partition in partitions.items() if partition.get("computation_id") == "reviser_like"]

    def _reviser_partition_states(self):
        partitions = self.client.get_flow_view(
            self.pipeline_path, view_path="/state/execution_spec/layout/partitions", cache=False
        )
        return {
            pid: partition.get("state")
            for pid, partition in partitions.items()
            if partition.get("computation_id") == "reviser_like"
        }

    def _max_visit_index_per_key(self):
        result = {}
        for row in self.get_output():
            result[row["key"]] = max(result.get(row["key"], 0), row["visit_index"])
        return result

    # A computation whose only work-source is key_visitor_streams (no inputs, no
    # sources) must be accepted, range-partitioned into the requested count, and
    # produce a visit per key seeded directly into the external state. Standalone,
    # so the partitions keep sweeping for the whole test instead of retiring after
    # their single pass.
    @pytest.mark.authors(["blinkov"])
    def test_key_visitor_only_accepted_and_partitioned(self):
        run_yt_sync("primary", self.work_yt_path, with_external_state=True)

        entries = [(f"k_{i:03d}", f"v_{i}") for i in range(20)]
        expected_keys = {k for k, _ in entries}

        pipeline_config_path = self.prepare_pipeline_keyvisitor_only_config(
            period_ms=1000, desired_partition_count=4, finite=False
        )
        with self.start_flow_process_federation(
            binary_path=self.FLOW_KEYVISITOR_ONLY_BINARY_PATH,
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
        run_yt_sync("primary", self.work_yt_path, with_external_state=True)

        entries = [(f"k_{i:03d}", f"v_{i}") for i in range(10)]
        expected_keys = {k for k, _ in entries}

        pipeline_config_path = self.prepare_pipeline_keyvisitor_only_config(
            period_ms=1000, desired_partition_count=2, finite=False
        )
        with self.start_flow_process_federation(
            binary_path=self.FLOW_KEYVISITOR_ONLY_BINARY_PATH,
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.wait_pipeline_state("working", timeout=120)
            self.seed_user_state(entries)

            # visit_index counts visits per key in the external state, so three visits of every
            # key mean the visitor rotated its pass at least twice — one rotation past the point
            # where the old code marked a pass final.
            def keys_swept(times):
                return {key for key, index in self._max_visit_index_per_key().items() if index >= times}

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
        run_yt_sync("primary", self.work_yt_path, with_external_state=True)

        entries = [(f"k_{i:03d}", f"v_{i}") for i in range(10)]
        expected_keys = {k for k, _ in entries}

        # Seeded before the pipeline starts: there is exactly one sweep, so a range the
        # visitor covers before the rows land would never be revisited.
        self.seed_user_state(entries)

        pipeline_config_path = self.prepare_pipeline_keyvisitor_only_config(
            period_ms=1000, desired_partition_count=2, finite=True
        )
        with self.start_flow_process_federation(
            binary_path=self.FLOW_KEYVISITOR_ONLY_BINARY_PATH,
            pipeline_binary_args={"--config": pipeline_config_path},
        ):

            def all_partitions_retired():
                states = self._reviser_partition_states()
                return bool(states) and set(states.values()) == {"completed"}

            wait(all_partitions_retired, timeout=120, ignore_exceptions=True)
            assert all_partitions_retired(), f"partitions did not retire: {self._reviser_partition_states()}"

            visits = self._max_visit_index_per_key()
            assert set(visits) == expected_keys, f"keys missed by the final pass: {expected_keys - set(visits)}"
            swept_twice = {key: index for key, index in visits.items() if index > 1}
            assert not swept_twice, f"keys swept more than once before retiring: {swept_twice}"

    # A standalone visitor has no end of input of its own, so the test says when it should
    # stop: flipping `finite` on the running pipeline makes the visitor finish its sweep and
    # retire. That is a real end-of-work barrier, which stopping the pipeline would not be —
    # a drain does not wait for unread input.
    @pytest.mark.authors(["sergeypozdeev"])
    def test_key_visitor_standalone_completes_after_flip(self):
        run_yt_sync("primary", self.work_yt_path, with_external_state=True)

        entries = [(f"k_{i:03d}", f"v_{i}") for i in range(10)]
        expected_keys = {k for k, _ in entries}

        pipeline_config_path = self.prepare_pipeline_keyvisitor_only_config(
            period_ms=1000, desired_partition_count=2, finite=False
        )
        with self.start_flow_process_federation(
            binary_path=self.FLOW_KEYVISITOR_ONLY_BINARY_PATH,
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.wait_pipeline_state("working", timeout=120)
            self.seed_user_state(entries)

            # The work the test came for, observed while the visitor is still unbounded.
            wait(
                lambda: set(self._max_visit_index_per_key()) >= expected_keys,
                timeout=60,
                ignore_exceptions=True,
            )
            assert set(self._max_visit_index_per_key()) >= expected_keys

            self.ask_key_visitor_to_complete("reviser_like", "visit_iter")
            self.wait_pipeline_state("completed", timeout=120)

            states = self._reviser_partition_states()
            assert set(states.values()) == {"completed"}, f"partitions did not retire: {states}"

    @pytest.mark.authors(["mikari"])
    def test_key_visitor(self):
        run_yt_sync("primary", self.work_yt_path)

        v1 = [(f"k_{i:03d}", f"v1_{i}") for i in range(20)]
        v2 = [(f"k_{i:03d}", f"v2_{i}") for i in range(20)]
        expected_keys = {k for k, _ in v1}
        expected_latest = {k: p for k, p in v2}

        pipeline_config_path = self.prepare_pipeline_config(period_ms=20000, finite=True)
        self.send_keys(v1)
        self.send_keys(v2)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
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
                assert (
                    actual_payload == expected_payload
                ), f"key={key!r}: latest visit had payload {actual_payload!r}, expected {expected_payload!r}"
            logging.info("cpp key_visitor passed (rows=%d)", sum(1 for _ in latest))

    @pytest.mark.authors(["mikari"])
    def test_key_visitor_sweeps_external_state_manager(self):
        run_yt_sync("primary", self.work_yt_path, with_external_state=True)

        v1 = [(f"k_{i:03d}", f"v1_{i}") for i in range(20)]
        v2 = [(f"k_{i:03d}", f"v2_{i}") for i in range(20)]
        expected_keys = {k for k, _ in v1}
        expected_latest = {k: p for k, p in v2}

        pipeline_config_path = self.prepare_pipeline_external_config(period_ms=20000, finite=True)
        self.send_keys(v1)
        self.send_keys(v2)
        with self.start_flow_process_federation(
            binary_path=self.FLOW_EXTERNAL_BINARY_PATH,
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.wait_pipeline_state("completed", timeout=240)

            latest = {}
            for row in self.get_output():
                idx = row["visit_index"]
                if idx > latest.get(row["key"], (-1, None))[0]:
                    latest[row["key"]] = (idx, row["payload"])

            missing = expected_keys - set(latest)
            assert not missing, (
                f"pipeline reached `completed` but {len(missing)} seeded keys were never visited via "
                f"external state: {sorted(missing)[:10]}{'...' if len(missing) > 10 else ''}"
            )
            for key, expected_payload in expected_latest.items():
                actual_payload = latest[key][1]
                assert (
                    actual_payload == expected_payload
                ), f"key={key!r}: latest visit had payload {actual_payload!r}, expected {expected_payload!r}"

            logging.info("cpp key_visitor external-state sweep passed (rows=%d)", sum(1 for _ in latest))

    # --- helpers for the repartition test -------------------------------------

    def _set_tester_partition_count(self, count):
        dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
        tester = dynamic_spec["spec"]["computations"]["tester"]
        tester.setdefault("parameters", {})["desired_partition_count"] = count
        self.client.set_pipeline_dynamic_spec(
            self.pipeline_path, dynamic_spec["spec"], expected_version=dynamic_spec["version"]
        )

    def _tester_partition_ids(self):
        partitions = self.client.get_flow_view(
            self.pipeline_path, view_path="/state/execution_spec/layout/partitions", cache=False
        )
        return [pid for pid, partition in partitions.items() if partition.get("computation_id") == "tester"]

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
        run_yt_sync("primary", self.work_yt_path)

        # Infinite source + short visitor period so successors sweep aggressively
        # during the interrupt window; start at a single partition so each
        # repartition interrupts a partition whose range overlaps every successor.
        pipeline_config_path = self.prepare_pipeline_config(period_ms=500, finite=False, desired_partition_count=1)
        with self.start_flow_process_federation(
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
