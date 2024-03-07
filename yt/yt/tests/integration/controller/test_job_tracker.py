from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    SCHEDULERS_SERVICE,
    NODES_SERVICE,
    CONTROLLER_AGENTS_SERVICE,
)

from yt_commands import (
    authors, run_sleeping_vanilla, get, ls, wait,
    set_node_banned, exists,
    run_test_vanilla, with_breakpoint,
    wait_breakpoint, release_breakpoint,
    update_controller_agent_config, update_nodes_dynamic_config,
    update_scheduler_config
)
from yt_helpers import read_structured_log, write_log_barrier, JobCountProfiler, profiler_factory

import pytest

import time

##################################################################


class TestJobTracker(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "node_heartbeat_timeout": 100000,
            "node_registration_timeout": 100000,
            "missing_jobs_check_period": 1000000,
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "job_tracker": {
                "node_disconnection_timeout": 500,
                "job_confirmation_timeout": 500,
            },
            "snapshot_period": 3000,
        },
    }

    def setup_method(self, method):
        super(TestJobTracker, self).setup_method(method)

        controller_agent_address = ls("//sys/controller_agents/instances")[0]
        self.from_barrier = write_log_barrier(controller_agent_address)

    def _get_job_tracker_orchid_path(self, controller_agent):
        return "//sys/controller_agents/instances/{}/orchid/controller_agent/job_tracker".format(controller_agent)

    def _get_controller_agent(self, op):
        return get(op.get_path() + "/@controller_agent_address")

    def _get_job_info(self, op, job_id):
        return get(
            self._get_job_tracker_orchid_path(self._get_controller_agent(op)) + "/jobs/{}".format(
                job_id))

    def _does_job_exists(self, op, job_id):
        return exists(
            self._get_job_tracker_orchid_path(self._get_controller_agent(op)) + "/jobs/{}".format(
                job_id))

    def _get_operation_info(self, op):
        return get(
            self._get_job_tracker_orchid_path(self._get_controller_agent(op)) + "/operations/{}".format(
                op.id))

    def _get_node_jobs(self, op, node_address):
        return get(
            self._get_job_tracker_orchid_path(self._get_controller_agent(op)) + "/nodes/{}".format(
                node_address))

    def _get_job_events_from_event_log(self, operation_id, controller_agent_address):
        assert self.NUM_CONTROLLER_AGENTS == 1

        controller_agent_log_file = self.path_to_run + "/logs/controller-agent-0.json.log"

        events = read_structured_log(controller_agent_log_file, from_barrier=self.from_barrier,
                                     row_filter=lambda e: "event_type" in e and e.get("operation_id") == operation_id)

        return events

    @authors("pogorelov")
    def test_job_tracker_orchid(self):
        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        op = run_sleeping_vanilla()

        op.ensure_running()

        controller_agent_address = self._get_controller_agent(op)

        wait(
            lambda: len(ls(
                self._get_job_tracker_orchid_path(controller_agent_address) + "/nodes")
            ) == self.NUM_NODES
        )

        wait(
            lambda: len(ls(
                self._get_job_tracker_orchid_path(controller_agent_address) + "/operations")
            ) == 1
        )

        wait(lambda: len(self._get_operation_info(op)["jobs"]) != 0)

        operation_info = self._get_operation_info(op)
        assert len(operation_info["jobs"]) == 1

        job_id = operation_info["jobs"][0]

        job_info = self._get_job_info(op, job_id)

        node_address = job_info["node_address"]
        assert op.get_node(job_id) == node_address

        node_jobs = self._get_node_jobs(op, node_address)

        assert len(node_jobs["jobs"]) == 1
        assert len(node_jobs["jobs_waiting_for_confirmation"]) == 0
        assert len(node_jobs["jobs_to_release"]) == 0
        assert len(node_jobs["jobs_to_abort"]) == 0

        node_running_jobs = node_jobs["jobs"]

        assert {
            "stage": "running",
            "operation_id": op.id,
        }.items() <= node_running_jobs[job_id].items()

        assert {
            "stage": "running",
            "operation_id": op.id,
        }.items() <= job_info.items()

        assert len(ls(
            self._get_job_tracker_orchid_path(controller_agent_address) + "/jobs")
        ) == 1

        op.abort()

        wait(
            lambda: len(ls(
                self._get_job_tracker_orchid_path(controller_agent_address) + "/operations")
            ) == 0
        )

        assert len(ls(
            self._get_job_tracker_orchid_path(controller_agent_address) + "/jobs")
        ) == 0

    @authors("pogorelov")
    def test_node_disconnection(self):
        op = run_sleeping_vanilla()

        op.ensure_running()

        wait(lambda: len(self._get_operation_info(op)["jobs"]) != 0)

        controller_agent_address = self._get_controller_agent(op)

        assert len(ls(
            self._get_job_tracker_orchid_path(controller_agent_address) + "/nodes")
        ) == self.NUM_NODES

        operation_info = self._get_operation_info(op)
        job_id = operation_info["jobs"][0]

        job_info = self._get_job_info(op, job_id)

        node_address = job_info["node_address"]
        set_node_banned(node_address, True)

        wait(
            lambda: not exists(
                self._get_job_tracker_orchid_path(controller_agent_address) + "/nodes/{}".format(
                    node_address)
            )
        )

        assert job_id not in ls(
            self._get_job_tracker_orchid_path(controller_agent_address) + "/jobs"
        )

        events = self._get_job_events_from_event_log(op.id, controller_agent_address)
        aborted_job_events = {event["job_id"]: event for event in events if event["event_type"] == "job_aborted"}

        assert len(aborted_job_events) == 1
        assert aborted_job_events[job_id]["reason"] == "node_offline"

        set_node_banned(node_address, False)

    @authors("pogorelov")
    def test_job_finish(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            spec={
                "testing": {
                    "delay_inside_operation_commit": 1000,
                    "delay_inside_operation_commit_stage": "start",
                }
            },
        )

        op.ensure_running()

        controller_agent_address = self._get_controller_agent(op)

        op.wait_for_fresh_snapshot()

        update_controller_agent_config("enable_snapshot_building", False)

        (job_id, ) = wait_breakpoint()

        job_info = self._get_job_info(op, job_id)

        assert job_info["stage"] == "running"

        job_orchid = op.get_job_node_orchid(job_id)
        assert not job_orchid["stored"]

        update_controller_agent_config("job_tracker/node_disconnection_timeout", 30000)

        update_nodes_dynamic_config({
            "exec_node": {
                "controller_agent_connector": {
                    "heartbeat_executor": {
                        "period": 3000,
                    }
                },
            },
        })

        release_breakpoint()

        node_job_orchid_path = "//sys/cluster_nodes/{0}/orchid/exec_node/job_controller/active_jobs/{1}".format(job_info["node_address"], job_id)

        wait(lambda: get(node_job_orchid_path)["stored"])

        assert self._get_job_info(op, job_id)["stage"] == "finished"

        update_controller_agent_config("enable_snapshot_building", True)
        op.track()

        wait(lambda: "release_flags" in self._get_job_info(op, job_id), ignore_exceptions=True)

        events = self._get_job_events_from_event_log(op.id, controller_agent_address)

        completed_job_events = {event["job_id"]: event for event in events if event["event_type"] == "job_completed"}

        assert len(completed_job_events) == 1
        assert job_id in completed_job_events

        update_nodes_dynamic_config({
            "exec_node": {
                "controller_agent_connector": {
                    "heartbeat_executor": {
                        "period": 100,
                    }
                },
            },
        })

        wait(lambda: not self._does_job_exists(op, job_id))

    @authors("pogorelov")
    def test_job_revival(self):
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))

        op.ensure_running()

        (job_id, ) = wait_breakpoint()

        op.wait_for_fresh_snapshot()

        update_controller_agent_config("job_tracker/job_confirmation_timeout", 30000)

        update_controller_agent_config("job_tracker/node_disconnection_timeout", 30000)

        update_nodes_dynamic_config({
            "exec_node": {
                "controller_agent_connector": {
                    "test_heartbeat_delay": 9000,
                },
            },
        })

        time.sleep(1)
        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        wait(lambda: self._get_job_info(op, job_id)["stage"] == "confirmation", ignore_exceptions=True)

        update_nodes_dynamic_config({
            "exec_node": {
                "controller_agent_connector": {
                    "test_heartbeat_delay": 0,
                },
            },
        })

        wait(lambda: self._get_job_info(op, job_id)["stage"] == "running")

        release_breakpoint()

        op.track()

    @authors("pogorelov")
    @pytest.mark.parametrize("mode", ["unconfirmed", "confirmation_timeout"])
    def test_unconfirmed_jobs(self, mode):
        (job_confirmation_timeout, job_abort_reason) = (30000, "unconfirmed") if mode == "unconfirmed" else (5000, "revival_confirmation_timeout")

        aborted_job_profiler = JobCountProfiler("aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": job_abort_reason})

        update_controller_agent_config("job_tracker/node_disconnection_timeout", 30000)
        update_controller_agent_config("job_tracker/job_confirmation_timeout", job_confirmation_timeout)

        update_scheduler_config("nodes_attributes_update_period", 1000000)

        time.sleep(1)

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))

        op.ensure_running()

        (job_id, ) = wait_breakpoint()

        op.wait_for_fresh_snapshot()

        update_nodes_dynamic_config({
            "exec_node": {
                "controller_agent_connector": {
                    "test_heartbeat_delay": 5000,
                },
            },
        })

        with Restarter(self.Env, NODES_SERVICE):
            pass

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        if mode != "confirmation_timeout":
            update_nodes_dynamic_config({
                "exec_node": {
                    "controller_agent_connector": {
                        "test_heartbeat_delay": 0,
                    },
                },
            })

        wait(lambda: aborted_job_profiler.get_job_count_delta() == 1)

        if mode == "confirmation_timeout":
            update_nodes_dynamic_config({
                "exec_node": {
                    "controller_agent_connector": {
                        "test_heartbeat_delay": 0,
                    },
                },
            })

        release_breakpoint()

        op.track()

    @authors("pogorelov")
    def test_abort_disappeared_from_node_job(self):
        update_controller_agent_config("job_tracker/node_disconnection_timeout", 100000)
        update_controller_agent_config("job_tracker/duration_before_job_considered_disappeared_from_node", 100)

        update_scheduler_config("nodes_attributes_update_period", 1000000)
        time.sleep(0.5)

        aborted_job_profiler = JobCountProfiler("aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "disappeared_from_node"})

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))

        (job_id, ) = wait_breakpoint()

        with Restarter(self.Env, NODES_SERVICE):
            pass

        wait(lambda: aborted_job_profiler.get_job_count_delta() == 1)

        release_breakpoint()

        op.track()

    @authors("arkady-e1ppa")
    def test_running_jobs_throttling(self):
        update_controller_agent_config("job_events_total_time_threshold", 0)
        controller_agent = ls("//sys/controller_agents/instances")[0]
        profiler = profiler_factory().at_controller_agent(controller_agent)
        throttled_running_job_event_counter = profiler.counter("controller_agent/job_tracker/node_heartbeat/throttled_running_job_event_count")
        throttled_heartbeat_counter = profiler.counter("controller_agent/job_tracker/node_heartbeat/throttled_heartbeat_count")
        throttled_operation_count = profiler.counter("controller_agent/job_tracker/node_heartbeat/throttled_operation_count")

        op = run_test_vanilla("sleep 1", job_count=1)

        wait(lambda: throttled_heartbeat_counter.get_delta() > 0)
        assert throttled_running_job_event_counter.get_delta() > 0
        assert throttled_operation_count.get_delta() > 0

        update_controller_agent_config("job_events_total_time_threshold", 1000)

        op.track()

##################################################################
