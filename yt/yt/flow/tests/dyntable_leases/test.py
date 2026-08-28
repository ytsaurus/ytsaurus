"""End-to-end run of a pipeline with the dyntable lease backend: leader election over the leases
dynamic table (no Cypress lock), controller transactions fenced by the leader row, worker commits
fenced by partition lease rows (no YT lease transactions at all).
"""

import logging

import pytest

from yt.common import wait

from yt.yt.flow.library.python.bullied_process import ProblemsConfig
from yt.yt.flow.tests.computation_cycles_and_buffers.lib.test_base import TestBase, EVENT_COUNT

##################################################################

DYNTABLE_LEASES_NODE_CONFIG = {
    "controller": {
        "election_manager": {
            "backend": "dyntable",
            "leader_lease_ttl": "15s",
            "detach_timeout": "15s",
            "lock_acquisition_period": "500ms",
        },
        # Every fenced transaction writes the leader lease row, so a spec update races the
        # scheduling cycle for it. The harness runs that cycle at 100ms — an order of magnitude
        # denser than production — which turns an occasional race into a near-certain one, and the
        # three attempts of the default budget run out. The retries themselves are the tested
        # behaviour; the wider budget only compensates for the test's cadence.
        "controller_service": {
            "set_spec_retry_count": 10,
            "set_spec_retry_period": "300ms",
        },
    },
}

##################################################################

# The single row that carries the deadline shared by every lease of the pipeline. It is not a
# lease, so it survives every revocation and must be excluded wherever leases are counted.
DEADLINE_ROW = ("", "expiration")

##################################################################


class Test(TestBase):

    def read_leases(self):
        """The leases table as {partition_id: {subkey: job_id}}, plus the deadline instant."""
        leases_path = self.pipeline_path + "/leases"
        deadline = None
        rows_by_partition = {}
        for row in self.client.select_rows(f"key, subkey, value FROM [{leases_path}]"):
            if (row["key"], row["subkey"]) == DEADLINE_ROW:
                deadline = row["value"]["expiration_instant"]
                continue
            rows_by_partition.setdefault(row["key"], {})[row["subkey"]] = row["value"]["job_id"]
        return rows_by_partition, deadline

    def owned_leases(self):
        """{partition_id: job_id} for the leases a job can actually commit under.

        A lease counts as held only if both rows are there and name the same job, which is exactly
        what ValidateAndTouchPartitionLease demands of the worker: a partition left with one row
        is mid-revocation and fences nobody.
        """
        rows_by_partition, _ = self.read_leases()
        owned = {}
        for partition_id, rows in rows_by_partition.items():
            job_ids = set(rows.values())
            if set(rows) == {"existence", "expiration"} and len(job_ids) == 1:
                owned[partition_id] = job_ids.pop()
        return owned

    @pytest.mark.authors(["thenewone"])
    @pytest.mark.parametrize("controllers_count", [1, 3], ids=["1c", "3c"])
    def test_pipeline_completes(self, controllers_count):
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config(finite=True)
        federation = self.start_flow_process_federation(
            node_config=dict(DYNTABLE_LEASES_NODE_CONFIG),
            pipeline_binary_args={
                "--config": pipeline_config_path,
            },
            workers_count=2,
            controllers_count=controllers_count,
        )

        with federation:
            self.wait_pipeline_state("completed", timeout=180)
            logging.info("pipeline completed over dyntable leases")

            leases_path = self.pipeline_path + "/leases"
            assert self.client.exists(leases_path)

            # Completion unassigns every job, and their leases must be revoked as part of that
            # transition rather than left to expire: a delayed worker holding a live lease could
            # still commit state and output after the pipeline is seen as completed.
            owned = self.owned_leases()
            assert owned == {}, owned

            _, deadline = self.read_leases()
            assert deadline is not None

            # The leader lease row is written to flow_control, not leases.
            flow_control_path = self.pipeline_path + "/flow_control"
            assert self.client.exists(flow_control_path)
            rows = list(self.client.select_rows(f"key FROM [{flow_control_path}]"))
            assert any(row["key"] == "leader_lease" for row in rows), rows

            # No job lease transactions must have been created: every job ran with a null lease
            # id and the dyntable fencing flag.
            flow_view = self.client.get_flow_view(self.pipeline_path)
            jobs = flow_view["state"]["execution_spec"]["layout"].get("jobs", {})
            for job in jobs.values():
                assert job["lease_id"] == "0-0-0-0", job
                assert job.get("dyntable_lease", False), job

    @pytest.mark.authors(["thenewone"])
    def test_fencing_holds_under_process_chaos(self):
        """The leases exist for the cases where processes misbehave, so this test creates them:
        controllers and workers are killed, restarted and frozen with SIGSTOP for longer than the
        partition lease timeout, which means a thawed worker returns holding a lease the
        controller has already revoked and reassigned. The result must still be exactly once, and
        the completion must leave no lease rows behind."""
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config(finite=True)
        federation = self.start_flow_process_federation(
            node_config=dict(DYNTABLE_LEASES_NODE_CONFIG),
            pipeline_binary_args={
                "--config": pipeline_config_path,
            },
            workers_count=3,
            controllers_count=3,
            # The budget is deliberately modest: the pipeline needs live workers to make
            # progress at all, and overlapping restarts of every worker at once starve it
            # instead of testing the fencing. The start delay lets it warm up first, and the
            # freeze is long enough to outlast the harness lease timeout of 15 seconds.
            controller_problems_config=ProblemsConfig(
                interval_seconds=15,
                problems_max_count=3,
                soft_restarts=True,
                stop_seconds=18,
                start_delay=20,
            ),
            worker_problems_config=ProblemsConfig(
                interval_seconds=15,
                problems_max_count=4,
                soft_restarts=True,
                stop_seconds=18,
                start_delay=20,
            ),
        )

        with federation:
            self.wait_pipeline_state("completed", timeout=480)

            rows = list(self.client.select_rows(f"* from [{self.state}]"))
            assert len(rows) == 1, rows
            # Exactly once across every kill, restart and freeze: no double counting from a
            # zombie worker that came back, no loss from a job that was reassigned.
            assert rows[0]["count"] == EVENT_COUNT, rows[0]

            owned = self.owned_leases()
            assert owned == {}, owned

    @pytest.mark.authors(["thenewone"])
    def test_repartitioning_keeps_leases_exact(self):
        """Repartitioning must leave the held leases exactly matching the live jobs: the removed
        partitions' leases are revoked, the new partitions get granted, and no partition outside
        the layout — or inside it under a stale job id — is left holding one."""
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config(finite=False)
        federation = self.start_flow_process_federation(
            node_config=dict(DYNTABLE_LEASES_NODE_CONFIG),
            pipeline_binary_args={
                "--config": pipeline_config_path,
            },
            workers_count=2,
            controllers_count=1,
        )

        with federation:
            self.wait_pipeline_state("working", timeout=180)

            def layout_jobs():
                view = self.client.get_flow_view(self.pipeline_path, cache=False)
                layout = view["state"]["execution_spec"]["layout"]
                partitions = layout.get("partitions", {})
                jobs = {job["partition_id"]: job_id for job_id, job in layout.get("jobs", {}).items()}
                transform = {pid for pid, p in partitions.items() if p["computation_id"] == "transform_a"}
                return jobs, transform

            def leases_are_exact(expected_transform_count):
                jobs, transform = layout_jobs()
                if len(transform) != expected_transform_count:
                    return False
                # The held leases name exactly the jobs of the layout — partition for partition
                # and job id for job id. A partition dropped by the repartitioning holding on to
                # its lease, or one whose lease still names the job it had before, would show up
                # here as a mismatch.
                return self.owned_leases() == jobs

            for count in (5, 6):
                self.client.set_pipeline_dynamic_spec(
                    self.pipeline_path,
                    count,
                    spec_path="/computations/transform_a/parameters/desired_partition_count",
                )
                wait(lambda: leases_are_exact(count), timeout=180, sleep_backoff=2)
                logging.info("leases exactly match the layout at desired_partition_count=%d", count)
