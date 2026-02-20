from yt_env_setup import YTEnvSetup, Restarter, SCHEDULERS_SERVICE, CONTROLLER_AGENTS_SERVICE

from yt_commands import (
    authors, execute_command, extract_statistic_v2, print_debug, wait, wait_breakpoint, release_breakpoint, with_breakpoint, events_on_fs,
    raises_yt_error, update_controller_agent_config, update_nodes_dynamic_config, update_scheduler_config,
    create, ls, exists, sorted_dicts, create_pool, get_job, list_jobs,
    get, write_file, read_table, write_table, vanilla, run_test_vanilla, abort_job, abandon_job,
    interrupt_job, dump_job_context, run_sleeping_vanilla, get_allocation_id_from_job_id,
    patch_op_spec)

from yt_helpers import profiler_factory, read_structured_log, skip_if_component_old, write_log_barrier, JobCountProfiler

from yt import yson
from yt.yson import to_yson_type
from yt.common import YtError, date_string_to_datetime

import pytest
from flaky import flaky

import datetime
import time
from collections import Counter
from copy import deepcopy
import builtins

##################################################################


def _get_controller_profiler():
    agent_addresses = ls("//sys/controller_agents/instances")
    assert len(agent_addresses) == 1

    return profiler_factory().at_controller_agent(agent_addresses[0])


def _get_job_tracker_orchid_path(op):
    controller_agent_address = op.get_controller_agent_address()
    return f"//sys/controller_agents/instances/{controller_agent_address}/orchid/controller_agent/job_tracker"


class TestSchedulerVanillaCommands(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_TEST_PARTITIONS = 3

    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 200,
        }
    }

    @authors("max42")
    def test_simple(self):
        master_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("master_job_started_${YT_JOB_COOKIE}"),
                events_on_fs().wait_event_cmd("finish"),
            ]
        )
        slave_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("slave_job_started_${YT_JOB_COOKIE}"),
                events_on_fs().wait_event_cmd("finish"),
            ]
        )

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "master": {
                        "job_count": 1,
                        "command": master_command,
                    },
                    "slave": {
                        "job_count": 2,
                        "command": slave_command,
                    },
                },
            },
        )

        wait(lambda: len(get(op.get_path() + "/@progress/tasks")) == 2, ignore_exceptions=True)

        # Ensure that all three jobs have started.
        events_on_fs().wait_event("master_job_started_0", timeout=datetime.timedelta(1000))
        events_on_fs().wait_event("slave_job_started_0", timeout=datetime.timedelta(1000))
        events_on_fs().wait_event("slave_job_started_1", timeout=datetime.timedelta(1000))

        events_on_fs().notify_event("finish")

        op.track()

        data_flow_graph_path = op.get_path() + "/@progress/data_flow_graph"
        get(data_flow_graph_path)
        assert get(data_flow_graph_path + "/vertices/master/job_type") == "vanilla"
        assert get(data_flow_graph_path + "/vertices/master/job_counter/completed/total") == 1
        assert get(data_flow_graph_path + "/vertices/slave/job_type") == "vanilla"
        assert get(data_flow_graph_path + "/vertices/slave/job_counter/completed/total") == 2

        tasks = {}
        for task in get(op.get_path() + "/@progress/tasks"):
            tasks[task["task_name"]] = task

        assert tasks["master"]["job_type"] == "vanilla"
        assert tasks["master"]["job_counter"]["completed"]["total"] == 1
        assert tasks["slave"]["job_type"] == "vanilla"
        assert tasks["slave"]["job_counter"]["completed"]["total"] == 2
        assert get(op.get_path() + "/@progress/total_job_counter/completed/total") == 3

    @authors("max42", "ignat")
    def test_task_job_index(self):
        master_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("job_started_master_${YT_TASK_JOB_INDEX}"),
                events_on_fs().wait_event_cmd("finish"),
            ]
        )

        slave_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("job_started_slave_${YT_TASK_JOB_INDEX}"),
                events_on_fs().wait_event_cmd("finish"),
            ]
        )

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "master": {
                        "job_count": 1,
                        "command": master_command,
                    },
                    "slave": {
                        "job_count": 3,
                        "command": slave_command,
                    },
                },
            },
        )

        # Ensure that all three jobs have started.
        events_on_fs().wait_event("job_started_master_0", timeout=datetime.timedelta(1000))
        events_on_fs().wait_event("job_started_slave_0", timeout=datetime.timedelta(1000))
        events_on_fs().wait_event("job_started_slave_1", timeout=datetime.timedelta(1000))
        events_on_fs().wait_event("job_started_slave_2", timeout=datetime.timedelta(1000))

        events_on_fs().notify_event("finish")

        op.track()

        data_flow_graph_path = op.get_path() + "/@progress/data_flow_graph"
        get(data_flow_graph_path)
        assert get(data_flow_graph_path + "/vertices/master/job_type") == "vanilla"
        assert get(data_flow_graph_path + "/vertices/master/job_counter/completed/total") == 1
        assert get(data_flow_graph_path + "/vertices/slave/job_type") == "vanilla"
        assert get(data_flow_graph_path + "/vertices/slave/job_counter/completed/total") == 3

        tasks = {}
        for task in get(op.get_path() + "/@progress/tasks"):
            tasks[task["task_name"]] = task

        assert tasks["master"]["job_type"] == "vanilla"
        assert tasks["master"]["job_counter"]["completed"]["total"] == 1
        assert tasks["slave"]["job_type"] == "vanilla"
        assert tasks["slave"]["job_counter"]["completed"]["total"] == 3
        assert get(op.get_path() + "/@progress/total_job_counter/completed/total") == 4

    @authors("max42")
    def test_files(self):
        create("file", "//tmp/a")
        write_file("//tmp/a", b"data_a")
        create("file", "//tmp/b")
        write_file("//tmp/b", b"data_b")

        vanilla(
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 1,
                        "command": 'if [[ `cat data` != "data_a" ]] ; then exit 1; fi',
                        "file_paths": [to_yson_type("//tmp/a", attributes={"file_name": "data"})],
                    },
                    "task_b": {
                        "job_count": 2,
                        "command": 'if [[ `cat data` != "data_b" ]] ; then exit 1; fi',
                        "file_paths": [to_yson_type("//tmp/b", attributes={"file_name": "data"})],
                    },
                },
                "max_failed_job_count": 1,
            }
        )

    @authors("max42")
    def test_stderr(self):
        create("table", "//tmp/stderr")

        op = vanilla(
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 3,
                        "command": 'echo "task_a" >&2',
                    },
                    "task_b": {
                        "job_count": 2,
                        "command": 'echo "task_b" >&2',
                    },
                },
                "stderr_table_path": "//tmp/stderr",
            }
        )

        table_stderrs = read_table("//tmp/stderr")
        table_stderrs_per_task = Counter(row["data"] for row in table_stderrs)

        job_ids = op.list_jobs()
        cypress_stderrs_per_task = Counter(op.read_stderr(job_id) for job_id in job_ids)

        assert dict(table_stderrs_per_task) == {"task_a\n": 3, "task_b\n": 2}
        assert dict(cypress_stderrs_per_task) == {b"task_a\n": 3, b"task_b\n": 2}

    @authors("max42")
    def test_fail_on_failed_job(self):
        with pytest.raises(YtError):
            vanilla(
                spec={
                    "tasks": {
                        "task_a": {
                            "job_count": 2,
                            "command": 'if [[ "$YT_JOB_INDEX" == 2 ]] ; then exit 1; fi',
                        },
                        "task_b": {
                            "job_count": 1,
                            "command": 'if [[ "$YT_JOB_INDEX" == 2 ]] ; then exit 1; fi',
                        },
                    },
                    "fail_on_job_restart": True,
                }
            )

    @authors("eshcherbin")
    def test_fail_on_job_restart_in_specific_task(self):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 1,
                        "command": events_on_fs().execute_once("exit 1"),
                    },
                    "task_b": {
                        "job_count": 1,
                        "command": "echo nothing >/dev/null",
                        "fail_on_job_restart": True,
                    },
                },
                "max_failed_job_count": 2,
            }
        )
        op.wait_for_state("completed")

        with pytest.raises(YtError):
            op = vanilla(
                spec={
                    "tasks": {
                        "task_a": {
                            "job_count": 1,
                            "command": events_on_fs().execute_once("exit 1"),
                            "fail_on_job_restart": True,
                        },
                        "task_b": {
                            "job_count": 1,
                            "command": "echo nothing >/dev/null",
                        },
                    },
                    "max_failed_job_count": 2,
                }
            )

    @authors("pogorelov")
    def test_revival_with_fail_on_job_restart_simple(self):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT"),
                    },
                    "task_b": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT"),
                    },
                },
                "fail_on_job_restart": True,
            },
        )
        wait(lambda: len(op.get_running_jobs()) == 2)
        op.wait_for_fresh_snapshot()
        # By this moment all 2 running jobs made it to snapshot, so operation will not fail on revival.
        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass
        release_breakpoint()
        op.track()

    @authors("pogorelov")
    def test_revival_with_fail_on_job_restart_with_completed_job(self):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT"),
                    },
                    "task_b": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT"),
                    },
                },
                "fail_on_job_restart": True,
            },
        )
        wait(lambda: len(op.get_running_jobs()) == 2)
        job_ids = wait_breakpoint(job_count=2)
        release_breakpoint(job_id=job_ids[0])
        op.wait_for_fresh_snapshot()
        # By this moment all 2 running jobs made it to snapshot, so operation will not fail on revival.
        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass
        release_breakpoint()
        op.track()

    @authors("pogorelov")
    def test_revival_with_fail_on_job_restart_failure(self):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_b": {
                        "job_count": 7,
                        "command": with_breakpoint("BREAKPOINT"),
                    },
                },
                "fail_on_job_restart": True,
            },
        )
        # 6 jobs may not be running simultaneously, so the snapshot will contain information about
        # at most 5 running jobs plus 1 completed job, leading to operation fail on revival.
        job_ids = wait_breakpoint(job_count=5)
        release_breakpoint(job_id=job_ids[0])
        wait(lambda: op.get_job_count("completed") == 1)
        op.wait_for_fresh_snapshot()
        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        op.wait_for_state("failed")

    @authors("max42")
    def test_abandon_job(self):
        # Abandoning vanilla job is ok.
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "tasks_a": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT ; exit 0"),
                    }
                },
                "fail_on_job_restart": True,
            },
        )
        job_id = wait_breakpoint()[0]
        jobs = op.get_running_jobs()
        assert len(jobs) == 1
        abandon_job(job_id)
        release_breakpoint()
        op.track()

    # TODO(max42): add lambda job: signal_job(job, "SIGKILL") when YT-8243 is fixed.
    @authors("max42")
    @pytest.mark.parametrize("action", [abort_job])
    def test_fail_on_manually_stopped_job(self, action):
        with pytest.raises(YtError):
            op = vanilla(
                track=False,
                spec={
                    "tasks": {
                        "task_a": {
                            "job_count": 1,
                            "command": " ; ".join(
                                [
                                    events_on_fs().notify_event_cmd("job_started_a"),
                                    events_on_fs().wait_event_cmd("finish_a"),
                                ]
                            ),
                        },
                        "task_b": {
                            "job_count": 1,
                            "command": " ; ".join(
                                [
                                    events_on_fs().notify_event_cmd("job_started_b"),
                                    events_on_fs().wait_event_cmd("finish_b"),
                                ]
                            ),
                        },
                    },
                    "fail_on_job_restart": True,
                },
            )
            events_on_fs().wait_event("job_started_a")
            events_on_fs().wait_event("job_started_b")
            jobs = list(op.get_running_jobs())
            assert len(jobs) == 2
            job_id = jobs[0]
            action(job_id)
            events_on_fs().notify_event("finish_a")
            events_on_fs().notify_event("finish_b")
            op.track()

    @authors("max42")
    def test_table_output(self):
        create("table", "//tmp/t_ab")  # append = %true
        create("table", "//tmp/t_bc_1")  # sorted_by = [a], sort_order=ascending
        create("table", "//tmp/t_bc_2")  # sorted_by = [a], sort_order=descending
        create("table", "//tmp/t_ac")  # regular
        write_table("//tmp/t_ab", [{"a": 1}])

        vanilla(
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 1,
                        "output_table_paths": [
                            "<append=%true>//tmp/t_ab",
                            "//tmp/t_ac",
                        ],
                        "command": "echo '{a=20}' >&1; echo '{a=9}' >&4",
                    },
                    "task_b": {
                        "job_count": 1,
                        "output_table_paths": [
                            "<sorted_by=[a]>//tmp/t_bc_1",
                            "<sorted_by=[{name=a;sort_order=descending}]>//tmp/t_bc_2",
                            "<append=%true>//tmp/t_ab",
                        ],
                        "command": "echo '{a=7}' >&1; echo '{a=7}' >&4; echo '{a=5}' >&7",
                    },
                    "task_c": {
                        "job_count": 1,
                        "output_table_paths": [
                            "//tmp/t_ac",
                            "<sorted_by=[a]>//tmp/t_bc_1",
                            "<sorted_by=[{name=a;sort_order=descending}]>//tmp/t_bc_2",
                        ],
                        "command": "echo '{a=3}' >&1; echo '{a=6}' >&4; echo '{a=6}' >&7",
                    },
                }
            }
        )
        assert read_table("//tmp/t_ab") in [
            [{"a": 1}, {"a": 20}, {"a": 5}],
            [{"a": 1}, {"a": 5}, {"a": 20}],
        ]
        assert get("//tmp/t_bc_1/@sorted")
        assert read_table("//tmp/t_bc_1") == [{"a": 6}, {"a": 7}]
        assert get("//tmp/t_bc_2/@sorted")
        assert read_table("//tmp/t_bc_2") == [{"a": 7}, {"a": 6}]
        assert read_table("//tmp/t_ac") in [[{"a": 3}, {"a": 9}], [{"a": 9}, {"a": 3}]]

    @authors("max42")
    def test_format(self):
        create("table", "//tmp/t")
        vanilla(
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 1,
                        "output_table_paths": ["//tmp/t"],
                        "format": "yson",
                        "command": "echo '{a=1}'",
                    },
                    "task_b": {
                        "job_count": 1,
                        "output_table_paths": ["//tmp/t"],
                        "format": "json",
                        "command": 'echo "{\\"a\\": 2}"',
                    },
                }
            }
        )
        assert sorted_dicts(read_table("//tmp/t")) == [{"a": 1}, {"a": 2}]

    @authors("faucct", "pogorelov")
    def test_job_collective(self):
        op = run_test_vanilla(
            with_breakpoint("echo YT_COLLECTIVE_MEMBER_RANK $YT_COLLECTIVE_MEMBER_RANK 1>&2; env 1>&2; BREAKPOINT"),
            task_patch={"collective_options": {"size": 2}}, job_count=1,
        )
        job_ids = wait_breakpoint(job_count=2)
        collective_id = list_jobs(op.id, attributes=["collective_id"])["jobs"][0]["collective_id"]

        jobs = [
            get_job(op.id, job_id, attributes=["job_id", "collective_member_rank", "collective_id"])
            for job_id in job_ids
        ]
        jobs = sorted(jobs, key=lambda job: job["collective_member_rank"])
        assert [0, 1] == [job["collective_member_rank"] for job in jobs]
        assert collective_id == jobs[0]["job_id"]
        assert {collective_id} == {job["collective_id"] for job in jobs}

        jobs = list_jobs(
            op.id, attributes=["job_id", "collective_member_rank", "collective_id"],
            collective_id=collective_id,
        )["jobs"]
        jobs = sorted(jobs, key=lambda job: job["collective_member_rank"])
        assert [0, 1] == [job["collective_member_rank"] for job in jobs]
        assert collective_id == jobs[0]["id"]
        assert {collective_id} == {job["collective_id"] for job in jobs}

        assert [] == list_jobs(
            op.id, attributes=["job_id", "collective_member_rank", "collective_id"],
            collective_id=(set(job_ids) - {collective_id}).pop(),
        )["jobs"]

        release_breakpoint()
        op.track()

    @authors("faucct", "pogorelov")
    def test_table_output_job_collective(self):
        create("table", "//tmp/t")
        with pytest.raises(YtError, match="echo: write error:"):
            vanilla(
                spec={
                    "tasks": {
                        "task_a": {
                            "job_count": 1,
                            "output_table_paths": ["//tmp/t"],
                            "format": "yson",
                            "command": """if [ "$YT_COLLECTIVE_MEMBER_RANK" == 0 ]; then sleep infinity; else echo "{foo=bar}"; fi""",
                            "collective_options": {"size": 2},
                            "close_stdout_if_unused": True,
                        },
                    },
                }
            )
        vanilla(
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 1,
                        "output_table_paths": ["//tmp/t"],
                        "format": "yson",
                        "command": """if [ "$YT_COLLECTIVE_MEMBER_RANK" == 0 ]; then echo '{a=1}'; fi""",
                        "collective_options": {"size": 2},
                        "close_stdout_if_unused": True,
                    },
                }
            }
        )
        assert sorted_dicts(read_table("//tmp/t")) == [{"a": 1}]

    @authors("max42")
    def test_attribute_validation_for_duplicated_output_tables(self):
        create("table", "//tmp/t")
        with pytest.raises(YtError):
            vanilla(
                spec={
                    "tasks": {
                        "task_a": {
                            "job_count": 1,
                            "command": "true",
                            "output_table_paths": ["<append=%true>//tmp/t"],
                        },
                        "task_b": {
                            "job_count": 1,
                            "command": "true",
                            "output_table_paths": ["<append=%false>//tmp/t"],
                        },
                    },
                    "fail_on_job_restart": True,
                }
            )

    @authors("max42")
    def test_operation_limits(self):
        with pytest.raises(YtError):
            vanilla(spec={"tasks": {"task_" + str(i): {"job_count": 1, "command": "true"} for i in range(101)}})
        with pytest.raises(YtError):
            vanilla(spec={"tasks": {"main": {"job_count": 100 * 1000 + 1, "command": "true"}}})

    @authors("dakovalkov", "max42")
    def test_restart_completed_jobs(self):
        op = run_test_vanilla(
            'if [[ "$YT_JOB_COOKIE" != "0" ]] ; then exit 1 ; else true ; fi',
            task_patch={"restart_completed_jobs": True},
            spec={"max_failed_job_count": 1},
        )

        def check_status():
            print_debug(op.build_progress())
            if op.get_job_count("lost") >= 3:
                return True
            if op.get_state(verbose=False) == "failed":
                raise op.get_error()
            return False

        wait(check_status)

    @authors("ignat")
    def test_get_job_context(self):
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        wait_breakpoint()

        jobs = list(op.get_running_jobs())
        assert len(jobs) == 1
        job_id = jobs[0]

        with pytest.raises(YtError):
            dump_job_context(job_id, "//tmp/input_context")

        release_breakpoint()

        wait(lambda: op.get_job_count("completed", from_orchid=False) == 1)
        assert op.get_job_count("aborted", from_orchid=False) == 0
        assert op.get_job_count("failed", from_orchid=False) == 0

    @authors("gritukan")
    def test_set_final_job_state_metrics(self):
        nodes = ls("//sys/cluster_nodes")

        counters = [profiler_factory().at_node(node).counter("job_controller/job_final_state") for node in nodes]
        op = run_test_vanilla("sleep 1")

        wait(lambda: any(counter.get_delta() > 0 for counter in counters))

        op.track()

    @authors("gritukan")
    def test_yt_job_cookie_in_env(self):
        create("table", "//tmp/stderr")

        vanilla(
            spec={
                "tasks": {
                    "task": {
                        "job_count": 3,
                        "command": 'echo $YT_JOB_COOKIE >&2; if [[ "$YT_JOB_INDEX" == 0 ]] ; then exit 1; fi',
                    },
                },
                "stderr_table_path": "//tmp/stderr",
            },
            fail_fast=False,
        )

        assert Counter(row["data"] for row in read_table("//tmp/stderr")) == {
            "0\n": 2,
            "1\n": 1,
            "2\n": 1,
        }

    @authors("gritukan")
    def test_empty_task_name(self):
        with pytest.raises(YtError):
            op = vanilla(
                spec={
                    "tasks": {
                        "": {
                            "job_count": 1,
                            "command": "echo A",
                        },
                    },
                }
            )
            op.track()

    @authors("ignat")
    def test_event_log_with_failed_on_job_restart(self):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 1,
                        "command": events_on_fs().notify_event_cmd("job_started_a") + " ; sleep 1000",
                    },
                    "task_b": {
                        "job_count": 1,
                        "command": events_on_fs().notify_event_cmd("job_started_b") + " ; sleep 1000",
                    },
                },
                "fail_on_job_restart": True,
            },
        )
        events_on_fs().wait_event("job_started_a")
        events_on_fs().wait_event("job_started_b")
        jobs = list(op.get_running_jobs())
        assert len(jobs) == 2
        job_id, other_job_id = jobs
        abort_job(job_id)

        wait(lambda: op.get_state() == "failed")

        now = datetime.datetime.utcnow()

        controller_agent_log_file = self.path_to_run + "/logs/controller-agent-0.json.log"
        controller_agent_address = ls("//sys/controller_agents/instances")[0]
        controller_agent_barrier = write_log_barrier(controller_agent_address)

        events = read_structured_log(controller_agent_log_file, to_barrier=controller_agent_barrier,
                                     row_filter=lambda e: "event_type" in e)
        aborted_job_events = {event["job_id"]: event for event in events if event["event_type"] == "job_aborted"}
        # NB: Scheduler can manage new scheduled job while CA is processing job abort.
        # So number of aborted jobs can be equal to 3.
        assert len(aborted_job_events) >= 2
        assert aborted_job_events[job_id]["reason"] == "user_request"
        assert aborted_job_events[other_job_id]["reason"] == "operation_failed"
        assert now - date_string_to_datetime(aborted_job_events[other_job_id]["finish_time"]) < datetime.timedelta(minutes=1)

    @authors("coteeq")
    def test_duplicate_output_tables(self):
        if self.Env.get_component_version("ytserver-controller-agent").abi <= (23, 2):
            pytest.skip()

        with raises_yt_error("Duplicate entries in output_table_paths"):
            vanilla(
                spec={
                    "tasks": {
                        "echo": {
                            "job_count": 1,
                            "command": "echo a=b",
                            "output_table_paths": [
                                "//tmp/out",
                                "//tmp/out",
                            ]
                        }
                    },
                }
            )

    @authors("coteeq")
    def test_job_proxy_memory(self):
        skip_if_component_old(self.Env, (25, 2), "controller-agent")
        update_controller_agent_config("footprint_memory", 42 * 1024 ** 2)
        create("table", "//tmp/out")

        op = vanilla(
            spec={
                "tasks": {
                    "echo": {
                        "job_count": 1,
                        "command": "echo '{a=b}'",
                        "output_table_paths": [
                            "//tmp/out",
                        ],
                        "job_io": {
                            "table_writer": {
                                "max_buffer_size": 100 * 1024 ** 2
                            }
                        }
                    }
                },
            }
        )

        statistics = get(op.get_path() + "/@progress/job_statistics_v2")

        memory_reserve = extract_statistic_v2(
            statistics,
            key="job_proxy.memory_reserve",
            job_type="echo",
            summary_type="sum")

        assert memory_reserve >= (100 + 42) * 1024 ** 2

    @authors("coteeq")
    def test_no_nul_in_file_name(self):
        skip_if_component_old(self.Env, (25, 2), "controller-agent")

        with raises_yt_error("must not contain"):
            run_sleeping_vanilla(
                track=True,
                task_patch={"file_paths": ['<file_name="with_\0_byte">//some/path']}
            )

    @authors("krasovav")
    def test_duplicate_volumes_and_disk_request(self):
        with raises_yt_error('Option "disk_request" cannot be specified simultaneously with "volumes"'):
            vanilla(
                spec={
                    "tasks": {
                        "a": {
                            "job_count": 1,
                            "command": "cat",
                            "disk_request": {
                                "disk_space": 1024 * 1024,
                            },
                            "volumes" : {
                                "a": {
                                    "disk_request": {
                                        "type": "tmpfs",
                                        "disk_space": 1024 * 1024,
                                    },
                                },
                            },
                        }
                    },
                }
            )

    @authors("krasovav")
    def test_duplicate_volumes_and_tmpfs_volumes(self):
        with raises_yt_error('Option "tmpfs_volumes" cannot be specified simultaneously with "volumes"'):
            vanilla(
                spec={
                    "tasks": {
                        "a": {
                            "job_count": 1,
                            "command": "cat",
                            "tmpfs_volumes": [
                                {
                                    "path": "tmpfs",
                                    "size": 1024 * 1024,
                                },
                            ],
                            "volumes" : {
                                "a": {
                                    "disk_request": {
                                        "type": "tmpfs",
                                        "disk_space": 1024 * 1024,
                                    },
                                },
                            },
                        }
                    },
                }
            )

    @authors("krasovav")
    def test_unused_volumes(self):
        with raises_yt_error('Volume was described but not used'):
            vanilla(
                spec={
                    "tasks": {
                        "a": {
                            "job_count": 1,
                            "command": "cat",
                            "volumes" : {
                                "a": {
                                    "disk_request": {
                                        "type": "tmpfs",
                                        "disk_space": 1024 * 1024,
                                    },
                                },
                            },
                        }
                    },
                }
            )

    @authors("krasovav")
    def test_not_described_volumes(self):
        with raises_yt_error('Volume was requested but not described'):
            vanilla(
                spec={
                    "tasks": {
                        "a": {
                            "job_count": 1,
                            "command": "cat",
                            "job_volumes_mounts" : [
                                {
                                    "volume_id": "a",
                                    "mount_path": "tmpfs",
                                },
                            ],
                        }
                    },
                }
            )

    # TODO(krasovav): Rewrite to check two different mediums after supporting two non tmpfs volumes.
    @authors("krasovav")
    def test_two_non_tmpfs_volumes(self):
        with raises_yt_error('Volume request with two or more different non tmpfs disk request are not currently supported'):
            vanilla(
                spec={
                    "tasks": {
                        "a": {
                            "job_count": 1,
                            "command": "cat",
                            "job_volumes_mounts" : [
                                {
                                    "volume_id": "a",
                                    "mount_path": "first",
                                },
                                {
                                    "volume_id": "b",
                                    "mount_path": "second",
                                },
                            ],
                            "volumes" : {
                                "a": {
                                    "disk_request": {
                                        "type": "local",
                                        "disk_space": 1024 * 1024,
                                    },
                                },
                                "b": {
                                    "disk_request": {
                                        "type": "local",
                                        "disk_space": 1024 * 1024,
                                    },
                                },
                            },
                        }
                    },
                }
            )


@pytest.mark.enabled_multidaemon
class TestYTDiscoveryServiceInVanilla(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    NUM_DISCOVERY_SERVERS = 2

    @authors("alex-shishkin")
    def test_yt_discovery_addresses_in_env(self):
        op = run_test_vanilla(
            "echo $YT_DISCOVERY_ADDRESSES >&2",
            task_patch={'extra_environment': ['discovery_server_addresses']},
            track=True
        )
        job_id = op.list_jobs()[0]
        addresses = yson.loads(op.read_stderr(job_id))
        assert len(addresses) == 2


class TestSchedulerVanillaCommandsMulticell(TestSchedulerVanillaCommands):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }


##################################################################


class TestVanillaOperationRevival(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.

    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 200,
        },
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 1,
            },
        },
    }

    @authors("pogorelov")
    def test_simple_revive(self):
        started_job_profiler = JobCountProfiler(
            "started",
            tags={"tree": "default", "job_type": "vanilla"},
        )

        incarnation_switch_counter = _get_controller_profiler().counter("controller_agent/gang_operations/incarnation_switch_count")

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="task_a"),
                    },
                    "task_b": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="task_b"),
                    },
                },
                "fail_on_job_restart": False,
            },
        )

        wait_breakpoint(breakpoint_name="task_a")
        wait_breakpoint(breakpoint_name="task_b")

        op.wait_for_fresh_snapshot()

        assert incarnation_switch_counter.get_delta() == 0

        wait(lambda: started_job_profiler.get_job_count_delta() == 2)
        started_job_profiler = JobCountProfiler(
            "started",
            tags={"tree": "default", "job_type": "vanilla"},
        )

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        release_breakpoint(breakpoint_name="task_a")
        release_breakpoint(breakpoint_name="task_b")

        op.track()

        assert started_job_profiler.get_job_count_delta() == 0
        assert incarnation_switch_counter.get_delta() == 0

    @authors("pogorelov")
    @pytest.mark.parametrize("jobs_were_scheduled", [0, 1])
    def test_revive_before_jobs_scheduled(self, jobs_were_scheduled):
        incarnation_switch_counter = _get_controller_profiler().with_tags({"reason": "job_lack_after_revival"}).counter(
            "controller_agent/gang_operations/incarnation_switch_count")

        started_job_profiler = JobCountProfiler(
            "started",
            tags={"tree": "default", "job_type": "vanilla"},
        )

        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/cpu")
        create_pool("test_pool", attributes={"strong_guarantee_resources": {"cpu": total_cpu_limit}})

        sleeping_op = run_sleeping_vanilla(spec={"pool": "test_pool"}, job_count=(3 - jobs_were_scheduled))
        wait(lambda: len(get(_get_job_tracker_orchid_path(sleeping_op) + f"/operations/{sleeping_op.id}/allocations")) == 3 - jobs_were_scheduled)

        # Will not start jobs while sleeping_op is running.
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            spec={"pool": "fake_pool"},
        )

        if jobs_were_scheduled:
            wait(lambda: len(get(_get_job_tracker_orchid_path(op) + f"/operations/{op.id}/allocations")) == jobs_were_scheduled)
        else:
            assert len(get(_get_job_tracker_orchid_path(op) + f"/operations/{op.id}/allocations")) == 0

        op.wait_for_fresh_snapshot()

        wait(lambda: started_job_profiler.get_job_count_delta() == 3)

        assert incarnation_switch_counter.get_delta() == 0

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            sleeping_op.abort()

        incarnation_switch_counter = _get_controller_profiler().with_tags({"reason": "job_lack_after_revival"}).counter(
            "controller_agent/gang_operations/incarnation_switch_count")

        started_job_profiler = JobCountProfiler(
            "started",
            tags={"tree": "default", "job_type": "vanilla"},
        )

        wait_breakpoint(job_count=3)
        release_breakpoint()

        op.track()

        wait(lambda: incarnation_switch_counter.get() == 0)
        wait(lambda: started_job_profiler.get(default=0) == 3 - jobs_were_scheduled)

    @authors("faucct", "pogorelov")
    def test_revive_before_job_collective_jobs_scheduled(self):
        started_job_profiler = JobCountProfiler(
            "started",
            tags={"tree": "default", "job_type": "vanilla"},
        )

        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/cpu")
        create_pool("test_pool", attributes={"min_share_resources": {"cpu": total_cpu_limit}})

        sleeping_op = run_sleeping_vanilla(spec={"pool": "test_pool"}, job_count=2)
        wait(lambda: len(get(_get_job_tracker_orchid_path(sleeping_op) + f"/operations/{sleeping_op.id}/allocations")) == 2)

        # Will not start jobs while sleeping_op is running.
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            task_patch={"collective_options": {"size": 3}},
            spec={"pool": "fake_pool"},
        )

        wait(lambda: len(get(_get_job_tracker_orchid_path(op) + f"/operations/{op.id}/allocations")) == 1)

        op.wait_for_fresh_snapshot()

        wait(lambda: started_job_profiler.get_job_count_delta() == 3)

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            sleeping_op.abort()

        started_job_profiler = JobCountProfiler(
            "started",
            tags={"tree": "default", "job_type": "vanilla"},
        )

        wait_breakpoint(job_count=3)
        assert op.get_job_count("aborted") == 1
        release_breakpoint()

        op.track()

        wait(lambda: started_job_profiler.get() == 3)


##################################################################

@pytest.mark.enabled_multidaemon
class TestSchedulerVanillaInterrupts(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    def _interrupt(self, op):
        jobs = list(op.get_running_jobs())
        assert len(jobs) == 1
        job_id = jobs[0]

        interrupt_job(job_id, interrupt_timeout=600000)
        return job_id

    @authors("ignat")
    @pytest.mark.parametrize("signal_name", ["SIGINT", "SIGUSR1"])
    def test_interrupt_root_process_only(self, signal_name):
        exit_code = 17
        command = """
bash -c 'trap "echo 'YYY'>&2; exit 0" {signal_name}; sleep 1000' &
child_pid="$!"

trap 'echo "XXX">&2; sleep 1; pkill -P $child_pid; kill $child_pid; exit {exit_code}' {signal_name};

BREAKPOINT

wait $child_pid
""".format(exit_code=exit_code, signal_name=signal_name)

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "tasks_a": {
                        "job_count": 1,
                        "command": with_breakpoint(command),
                        "interruption_signal": signal_name,
                        "signal_root_process_only": True,
                        "restart_exit_code": exit_code,
                    }
                },
                "max_failed_job_count": 1,
            },
        )
        wait_breakpoint()
        release_breakpoint()

        interrupted_job_id = self._interrupt(op)

        wait(lambda: op.get_job_count("lost") == 1)
        wait(lambda: op.get_job_count("running") == 1)

        assert b"XXX" in op.read_stderr(interrupted_job_id)

    @authors("max42")
    def test_non_interruptible(self):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "tasks_a": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT ; exit 0"),
                    }
                },
                "fail_on_job_restart": True,
            },
        )
        wait_breakpoint()
        jobs = list(op.get_running_jobs())
        assert len(jobs) == 1
        job_id = jobs[0]
        with pytest.raises(YtError):
            interrupt_job(job_id)

    @authors("ignat")
    @flaky(max_runs=3)  # More details in YT-15821.
    def test_successful_interrupts(self):
        exit_code = 17
        command = """(trap "exit {}" SIGINT; BREAKPOINT; trap "exit 0" SIGINT; sleep 100)""".format(exit_code)

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "tasks_a": {
                        "job_count": 1,
                        "command": with_breakpoint(command),
                        "interruption_signal": "SIGINT",
                        "restart_exit_code": exit_code,
                    }
                },
            },
        )
        wait_breakpoint()

        self._interrupt(op)

        wait(lambda: op.get_job_count("lost") == 1)
        wait(lambda: op.get_job_count("running") == 1)

        wait_breakpoint()
        release_breakpoint()

        # Give time to execute second trap command.
        time.sleep(5)

        self._interrupt(op)

        wait(lambda: op.get_job_count("completed") == 1 and op.get_job_count("lost") == 1)
        op.track()

    @authors("krasovav")
    def test_incorrect_interrupt_signal(self):
        with pytest.raises(YtError):
            vanilla(
                spec={
                    "tasks": {
                        "a": {
                            "job_count": 1,
                            "command": "cat",
                            "interruption_signal": "SIGINCORRECT",
                            "exit_code": 10,
                        }
                    },
                },
            )


@pytest.mark.enabled_multidaemon
class TestSchedulerVanillaInterruptsPorto(TestSchedulerVanillaInterrupts):
    ENABLE_MULTIDAEMON = True
    USE_PORTO = True


##################################################################

class TestGangOperations(YTEnvSetup):
    NUM_TEST_PARTITIONS = 3

    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 1000,

            "user_job_monitoring": {
                "extended_max_monitored_user_jobs_per_operation": 5,
                "max_monitored_user_jobs_per_agent": 10,
                "max_monitored_user_gangs_jobs_per_agent": 3,
            },
        },
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 1,
            },
        },
    }

    def _get_operation_incarnation(self, op):
        wait(lambda: exists(op.get_orchid_path() + "/controller/operation_incarnation"))
        incarnation_id = get(op.get_orchid_path() + "/controller/operation_incarnation")
        print_debug(f"Current incarnation of operation {op.id} is {incarnation_id}")

        return incarnation_id

    def _get_running_jobs(self, op, job_count):
        running_jobs = []

        def check():
            nonlocal running_jobs
            running_jobs = op.get_running_jobs()
            return len(running_jobs) == job_count

        wait(check)
        return running_jobs

    def _get_job_id_to_rank_map(self, running_jobs):
        return {job_id: info.get("gang_rank") for job_id, info in running_jobs.items()}

    def _get_allocation_id_to_rank_map(self, running_jobs):
        return {get_allocation_id_from_job_id(job_id): info.get("gang_rank") for job_id, info in running_jobs.items()}

    def _verify_job_ids_equal(self, running_jobs, job_ids):
        assert set(running_jobs.keys()) == set(job_ids)

    def _get_jobs_with_no_ranks(self, running_jobs):
        return [job_id for job_id, info in running_jobs.items() if info.get("gang_rank") is None]

    def _get_jobs_with_ranks(self, running_jobs):
        return [job_id for job_id, info in running_jobs.items() if info.get("gang_rank") is not None]

    @authors("pogorelov", "arkady-e1ppa")
    def test_operation_incarnation_is_set(self):
        started_gang_counter = _get_controller_profiler().counter("controller_agent/gang_operations/started_count")

        # NB(arkady-e1ppa): Die with code 42 if variable is not set.
        command = '[[ -z "$YT_OPERATION_INCARNATION" ]] && exit 42 || exit 0'
        op = vanilla(
            spec={
                "tasks": {
                    "test": {
                        "job_count": 1,
                        "command": command,
                        "gang_options": {},
                    },
                },
                "max_failed_job_count": 1,
            }
        )
        op.wait_for_state("completed")
        op.track()

        wait(lambda: started_gang_counter.get_delta() == 1)

    @authors("pogorelov")
    def test_restart_on_abortion(self):
        restarted_job_profiler = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla", "abort_reason": "operation_incarnation_changed"},
        )
        aborted_by_request_job_profiler = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla", "abort_reason": "user_request"},
        )

        incarnation_switch_counter = _get_controller_profiler().with_tags({"reason": "job_aborted"}).counter("controller_agent/gang_operations/incarnation_switch_count")

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={"gang_options": {}},
        )

        job_ids = wait_breakpoint(job_count=3)

        assert len(job_ids) == 3

        first_job_id = job_ids[0]

        print_debug("aborting job ", first_job_id)

        abort_job(first_job_id)

        wait(lambda: aborted_by_request_job_profiler.get_job_count_delta() == 1)
        wait(lambda: restarted_job_profiler.get_job_count_delta() == 2)

        job_orchid_addresses = [op.get_job_node_orchid_path(job_id) + f"/exec_node/job_controller/active_jobs/{job_id}" for job_id in job_ids]

        release_breakpoint(job_id=job_ids[0])
        release_breakpoint(job_id=job_ids[1])
        release_breakpoint(job_id=job_ids[2])

        for job_orchid_address in job_orchid_addresses:
            wait(lambda: not exists(job_orchid_address))

        new_job_ids = wait_breakpoint(job_count=3)

        wait(lambda: incarnation_switch_counter.get_delta() == 1)

        assert len(set(job_ids) & set(new_job_ids)) == 0

        allocation_ids = set([get_allocation_id_from_job_id(job_id) for job_id in job_ids])
        allocation_ids.remove(get_allocation_id_from_job_id(first_job_id))
        new_allocation_ids = set([get_allocation_id_from_job_id(job_id) for job_id in new_job_ids])

        assert allocation_ids.issubset(new_allocation_ids)

        release_breakpoint()

        op.track()

    @authors("pogorelov")
    def test_restart_on_job_failure(self):
        incarnation_switch_counter = _get_controller_profiler().with_tags({"reason": "job_failed"}).counter("controller_agent/gang_operations/incarnation_switch_count")

        restarted_job_profiler = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla", "abort_reason": "operation_incarnation_changed"},
        )
        failed_job_profiler = JobCountProfiler(
            "failed",
            tags={"tree": "default", "job_type": "vanilla"},
        )

        command = """
            BREAKPOINT;
            if [ "$YT_JOB_INDEX" -eq 1 ]; then
                exit 1
            else
                sleep 1000
            fi;
        """

        op = run_test_vanilla(
            with_breakpoint(command),
            job_count=3,
            spec={"max_failed_job_count": 2},
            task_patch={"gang_options": {}},
        )

        job_ids = wait_breakpoint(job_count=3)

        assert len(job_ids) == 3

        job_orchid_addresses = [op.get_job_node_orchid_path(job_id) + f"/exec_node/job_controller/active_jobs/{job_id}" for job_id in job_ids]

        release_breakpoint(job_id=job_ids[0])
        release_breakpoint(job_id=job_ids[1])
        release_breakpoint(job_id=job_ids[2])

        wait(lambda: failed_job_profiler.get_job_count_delta() == 1)
        wait(lambda: restarted_job_profiler.get_job_count_delta() == 2)

        wait(lambda: incarnation_switch_counter.get_delta() == 1)

        for job_orchid_address in job_orchid_addresses:
            wait(lambda: not exists(job_orchid_address))

        new_job_ids = wait_breakpoint(job_count=3)

        assert len(set(job_ids) & set(new_job_ids)) == 0

        allocation_ids = set([get_allocation_id_from_job_id(job_id) for job_id in job_ids])
        new_allocation_ids = set([get_allocation_id_from_job_id(job_id) for job_id in new_job_ids])

        print(f"New allocations are {new_allocation_ids}, old are {allocation_ids}")

        assert len(allocation_ids & new_allocation_ids) == 2

        op.abort()

    @authors("pogorelov")
    def test_abandon_job(self):
        incarnation_switch_counter = _get_controller_profiler().counter("controller_agent/gang_operations/incarnation_switch_count")
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=2,
            task_patch={"gang_options": {}},
        )

        job_id_to_abandon, job_id_to_continue = wait_breakpoint(job_count=2)

        print(f"Abandoning job {job_id_to_abandon}")
        abandon_job(job_id_to_abandon)

        release_breakpoint(job_id=job_id_to_continue)

        op.track()

        assert incarnation_switch_counter.get_delta() == 0

    @authors("pogorelov")
    @pytest.mark.parametrize("task_to_abort_job_of", ["task_a", "task_b"])
    def test_task_spec(self, task_to_abort_job_of):
        aborted_job_profiler = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla"},
        )

        restarted_job_profiler = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla", "abort_reason": "operation_incarnation_changed"},
        )

        incarnation_switch_counter = _get_controller_profiler().counter("controller_agent/gang_operations/incarnation_switch_count")

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="task_a"),
                    },
                    "task_b": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="task_b"),
                        "gang_options": {},
                    },
                },
                "fail_on_job_restart": False,
                "testing": {
                    "settle_job_delay": {
                        "duration": 1000 if task_to_abort_job_of == "task_b" else 0,
                        "type": "async",
                    },
                },
            },
        )

        (job_a, ) = wait_breakpoint(breakpoint_name="task_a")
        (job_b, ) = wait_breakpoint(breakpoint_name="task_b")

        if task_to_abort_job_of == "task_a":
            job_to_abort = job_a
        else:
            job_to_abort = job_b

        abort_job(job_to_abort)
        jobs_a = wait_breakpoint(job_count=2, breakpoint_name="task_a")

        if task_to_abort_job_of == "task_b":
            wait_breakpoint(job_count=2, breakpoint_name="task_b")

            assert get_allocation_id_from_job_id(jobs_a[0]) == get_allocation_id_from_job_id(jobs_a[1])

        release_breakpoint(breakpoint_name="task_a")
        release_breakpoint(breakpoint_name="task_b")

        op.track()

        if task_to_abort_job_of == "task_b":
            assert aborted_job_profiler.get_job_count_delta() == 2
            assert restarted_job_profiler.get_job_count_delta() == 1

            wait(lambda: incarnation_switch_counter.get_delta() == 1)
        else:
            assert aborted_job_profiler.get_job_count_delta() == 1
            assert restarted_job_profiler.get_job_count_delta() == 0

            assert incarnation_switch_counter.get_delta() == 0

    @authors("pogorelov")
    def test_multiple_incarnation_switches(self):
        incarnation_switch_counter = _get_controller_profiler().with_tags({"reason": "job_aborted"}).counter("controller_agent/gang_operations/incarnation_switch_count")

        incarnation_switch_count = 3

        restarted_job_profiler = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla", "abort_reason": "operation_incarnation_changed"},
        )
        aborted_by_request_job_profiler = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla", "abort_reason": "user_request"},
        )

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={"gang_options": {}},
        )

        previous_job_ids = []
        all_started_jobs = set()
        previous_operation_incarnations = set()

        last_job_ids = wait_breakpoint(job_count=3)
        all_started_jobs.update(last_job_ids)

        for iteration in range(incarnation_switch_count):
            previous_operation_incarnations.add(self._get_operation_incarnation(op))

            assert len(last_job_ids) == 3

            first_job_id = last_job_ids[0]

            print_debug("aborting job ", first_job_id)

            abort_job(first_job_id)

            wait(lambda: aborted_by_request_job_profiler.get_job_count_delta() == iteration + 1)
            wait(lambda: restarted_job_profiler.get_job_count_delta() == 2 * (iteration + 1))

            job_orchid_addresses = [op.get_job_node_orchid_path(job_id) + f"/exec_node/job_controller/active_jobs/{job_id}" for job_id in last_job_ids]

            release_breakpoint(job_id=last_job_ids[0])
            release_breakpoint(job_id=last_job_ids[1])
            release_breakpoint(job_id=last_job_ids[2])

            for job_orchid_address in job_orchid_addresses:
                wait(lambda: not exists(job_orchid_address))

            previous_job_ids = last_job_ids

            wait(lambda: incarnation_switch_counter.get_delta() == iteration + 1)

            last_job_ids = wait_breakpoint(job_count=3)
            assert len(set(last_job_ids) & all_started_jobs) == 0

            all_started_jobs.update(last_job_ids)

            allocation_ids = set([get_allocation_id_from_job_id(job_id) for job_id in previous_job_ids])
            allocation_ids.remove(get_allocation_id_from_job_id(first_job_id))
            new_allocation_ids = set([get_allocation_id_from_job_id(job_id) for job_id in last_job_ids])

            assert allocation_ids.issubset(new_allocation_ids)

            assert self._get_operation_incarnation(op) not in previous_operation_incarnations

        release_breakpoint()

        op.track()

    @authors("pogorelov")
    # NB(pogorelov): See YT-26422.
    # We are testing gang operation with allocation reusing after completed job.
    def test_gang_with_regular_allocation_reusing(self):
        update_nodes_dynamic_config(path="exec_node/job_controller/allocation/enable_multiple_jobs", value=True)

        incarnation_switch_counter = _get_controller_profiler().counter("controller_agent/gang_operations/incarnation_switch_count")

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=4,
            task_patch={"gang_options": {}},
            spec={"enable_multiple_jobs_in_allocation": True},
        )

        wait_breakpoint(job_count=3)

        release_breakpoint()

        op.track()

        assert incarnation_switch_counter.get_delta() == 0

    @authors("pogorelov")
    def test_simple_revive(self):
        aborted_job_profiler = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla"},
        )

        incarnation_switch_counter = _get_controller_profiler().counter("controller_agent/gang_operations/incarnation_switch_count")

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="task_a"),
                    },
                    "task_b": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="task_b"),
                        "gang_options": {},
                    },
                },
                "fail_on_job_restart": False,
            },
        )

        (job_a, ) = wait_breakpoint(breakpoint_name="task_a")
        (job_b, ) = wait_breakpoint(breakpoint_name="task_b")

        op.wait_for_fresh_snapshot()

        assert incarnation_switch_counter.get_delta() == 0

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        release_breakpoint(breakpoint_name="task_a")
        release_breakpoint(breakpoint_name="task_b")

        op.track()

        assert aborted_job_profiler.get_job_count_delta() == 0
        assert incarnation_switch_counter.get_delta() == 0

    @authors("pogorelov")
    @pytest.mark.parametrize("jobs_were_scheduled", [0, 1])
    def test_revive_before_jobs_scheduled(self, jobs_were_scheduled):
        incarnation_switch_counter = _get_controller_profiler().with_tags({"reason": "job_lack_after_revival"}).counter(
            "controller_agent/gang_operations/incarnation_switch_count")

        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/cpu")
        create_pool("test_pool", attributes={"strong_guarantee_resources": {"cpu": total_cpu_limit}})

        sleeping_op = run_sleeping_vanilla(spec={"pool": "test_pool"}, job_count=(3 - jobs_were_scheduled))
        wait(lambda: len(get(_get_job_tracker_orchid_path(sleeping_op) + f"/operations/{sleeping_op.id}/allocations")) == 3 - jobs_were_scheduled)

        # Will not start jobs while sleeping_op is running.
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={"gang_options": {}},
            spec={"pool": "fake_pool"},
        )

        first_incarnation_id = self._get_operation_incarnation(op)

        if jobs_were_scheduled:
            wait(lambda: len(get(_get_job_tracker_orchid_path(op) + f"/operations/{op.id}/allocations")) == jobs_were_scheduled)
        else:
            assert len(get(_get_job_tracker_orchid_path(op) + f"/operations/{op.id}/allocations")) == 0

        op.wait_for_fresh_snapshot()

        assert incarnation_switch_counter.get_delta() == 0

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            sleeping_op.abort()

        incarnation_switch_counter = _get_controller_profiler().with_tags({"reason": "job_lack_after_revival"}).counter(
            "controller_agent/gang_operations/incarnation_switch_count")

        second_incarnation_id = self._get_operation_incarnation(op)

        print_debug(f"First incarnation id: {first_incarnation_id}, second incarnation id: {second_incarnation_id}")

        assert first_incarnation_id != second_incarnation_id

        wait_breakpoint(job_count=3)
        release_breakpoint()

        op.track()

        wait(lambda: incarnation_switch_counter.get() == 1)

    @authors("pogorelov")
    @pytest.mark.parametrize("with_gang_policy", [False, True])
    def test_revive_with_completed_task(self, with_gang_policy):
        incarnation_switch_counter = _get_controller_profiler().with_tags({"reason": "job_lack_after_revival"}).counter(
            "controller_agent/gang_operations/incarnation_switch_count")

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="task_a"),
                    },
                    "task_b": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="task_b"),
                        "gang_options": {},
                    },
                },
                "fail_on_job_restart": False,
            },
        )

        job_id_a, = wait_breakpoint(job_count=1, breakpoint_name="task_a")
        job_id_b, = wait_breakpoint(job_count=1, breakpoint_name="task_b")

        print_debug(f"Job ids are task_a: {job_id_a}, task_b: {job_id_b}")

        first_incarnation_id = self._get_operation_incarnation(op)

        if with_gang_policy:
            release_breakpoint(job_id=job_id_b, breakpoint_name="task_b")
            task_to_wait_for_job_completed = "task_b"
            print_debug(f"Releasing breakpoint for job {job_id_b} (task_b)")
        else:
            release_breakpoint(job_id=job_id_a, breakpoint_name="task_a")
            task_to_wait_for_job_completed = "task_a"
            print_debug(f"Releasing breakpoint for job {job_id_a} (task_a)")

        def check_task_completed(task_name):
            tasks_progress = get(op.get_path() + "/controller_orchid/progress/tasks", verbose=True)

            for task in tasks_progress:
                if task["task_name"] != task_name:
                    continue

                return task["job_counter"]["completed"]["total"] == 1

            assert False, f"Task {task_name} not found"

        wait(lambda: check_task_completed(task_to_wait_for_job_completed))

        op.wait_for_fresh_snapshot()

        assert incarnation_switch_counter.get_delta() == 0

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        incarnation_switch_counter = _get_controller_profiler().with_tags({"reason": "job_lack_after_revival"}).counter(
            "controller_agent/gang_operations/incarnation_switch_count")

        # if with_gang_policy:
        #     release_breakpoint(job_id=job_id_a, breakpoint_name="task_a")
        # else:
        #     release_breakpoint(job_id=job_id_b, breakpoint_name="task_b")

        if with_gang_policy:
            job_id_a, = wait_breakpoint(job_count=1, breakpoint_name="task_a")
        job_id_b, = wait_breakpoint(job_count=1, breakpoint_name="task_b")

        second_incarnation_id = self._get_operation_incarnation(op)

        print_debug(f"First incarnation id: {first_incarnation_id}, second incarnation id: {second_incarnation_id}")

        if with_gang_policy:
            assert first_incarnation_id != second_incarnation_id
        else:
            assert first_incarnation_id == second_incarnation_id

        release_breakpoint(breakpoint_name="task_a")
        release_breakpoint(breakpoint_name="task_b")

        op.track()

        if with_gang_policy:
            wait(lambda: incarnation_switch_counter.get() == 1)
        else:
            assert incarnation_switch_counter.get() == 0

    @authors("pogorelov")
    def test_restart_completed_jobs(self):
        completed_job_profiler = JobCountProfiler(
            "completed",
            tags={"tree": "default", "job_type": "vanilla"},
        )

        incarnation_switch_counter = _get_controller_profiler().with_tags({"reason": "job_aborted"}).counter(
            "controller_agent/gang_operations/incarnation_switch_count")

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={"gang_options": {}},
        )

        job_ids = wait_breakpoint(job_count=3)

        print_debug("Running jobs {}; Completing {} and {}, restarting {}".format(job_ids, job_ids[0], job_ids[1], job_ids[2]))

        release_breakpoint(job_id=job_ids[0])
        release_breakpoint(job_id=job_ids[1])

        wait(lambda: completed_job_profiler.get_job_count_delta() == 2)

        abort_job(job_ids[2])
        release_breakpoint(job_id=job_ids[2])

        # wait(lambda: restarted_job_profiler.get_job_count_delta() == 2)

        wait(lambda: incarnation_switch_counter.get_delta() == 1)

        job_ids = wait_breakpoint(job_count=3)
        release_breakpoint()
        op.track()

    @authors("pogorelov")
    @pytest.mark.parametrize("with_job_revival", [False, True])
    def test_preserving_job_cookie_for_allocation(self, with_job_revival):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={"gang_options": {}},
        )

        job_ids = wait_breakpoint(job_count=3)
        assert len(job_ids) == 3

        if with_job_revival:
            op.wait_for_fresh_snapshot()
            with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
                pass

            op.wait_for_job_revival_finished()

        # We create it after CA restart to not compare new value of sensor with value before restart.
        restarted_job_profiler = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla", "abort_reason": "operation_incarnation_changed"},
        )

        first_job_id = job_ids[0]

        running_jobs = self._get_running_jobs(op, 3)
        print_debug(f"Running jobs are {running_jobs}")

        old_allocation_id_to_job_cookie = {get_allocation_id_from_job_id(job_id): info["job_cookie"] for job_id, info in running_jobs.items()}
        aborted_job_cookie = old_allocation_id_to_job_cookie[get_allocation_id_from_job_id(first_job_id)]

        print_debug(f"Old allocation id to job cookie are: {old_allocation_id_to_job_cookie}")

        print_debug(f"Aborting job {first_job_id} with cookie {aborted_job_cookie}")

        abort_job(first_job_id)

        wait(lambda: restarted_job_profiler.get_job_count_delta() == 2)

        job_orchid_addresses = [op.get_job_node_orchid_path(job_id) + f"/exec_node/job_controller/active_jobs/{job_id}" for job_id in job_ids]

        release_breakpoint(job_id=job_ids[0])
        release_breakpoint(job_id=job_ids[1])
        release_breakpoint(job_id=job_ids[2])

        for job_orchid_address in job_orchid_addresses:
            wait(lambda: not exists(job_orchid_address))

        new_job_ids = wait_breakpoint(job_count=3)

        assert len(set(job_ids) & set(new_job_ids)) == 0

        running_jobs = self._get_running_jobs(op, 3)
        print_debug(f"Running jobs are {running_jobs}")

        new_allocation_id_to_job_cookie = {get_allocation_id_from_job_id(job_id): info["job_cookie"] for job_id, info in running_jobs.items()}
        print_debug(f"New allocation id to job cookie are: {new_allocation_id_to_job_cookie}")

        assert new_allocation_id_to_job_cookie != old_allocation_id_to_job_cookie

        old_allocation_id_to_job_cookie.pop(get_allocation_id_from_job_id(first_job_id))

        for allocation_id, old_job_cookie in old_allocation_id_to_job_cookie.items():
            assert old_job_cookie == new_allocation_id_to_job_cookie[allocation_id]

        release_breakpoint()

        op.track()

    @authors("pogorelov")
    @pytest.mark.parametrize(
        "with_job_revival, use_operation_id_based_descriptors_for_gangs_jobs",
        [
            (False, False),
            (False, True),
            (True, False),
            (True, True)
        ],
    )
    def test_preserving_monitoring_descriptor_for_allocation(self, with_job_revival, use_operation_id_based_descriptors_for_gangs_jobs):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={
                "gang_options": {},
                "monitoring": {
                    "enable": True,
                    "use_operation_id_based_descriptors_for_gangs_jobs": use_operation_id_based_descriptors_for_gangs_jobs,
                },
            },
        )

        def check_gang_monitoring_descriptor(op, running_jobs, job_to_monitoring_descriptors):
            job_id_to_rank = self._get_job_id_to_rank_map(running_jobs)
            for job_id, descriptor in job_to_monitoring_descriptors.items():
                expected_job_descriptor = "{}/{}".format(op.id, job_id_to_rank[job_id] << 1)
                assert descriptor == expected_job_descriptor

        first_job_ids = wait_breakpoint(job_count=3)
        first_running_jobs = self._get_running_jobs(op, 3)
        self._verify_job_ids_equal(first_running_jobs, first_job_ids)
        assert len(first_job_ids) == 3

        print_debug(f"First job info is {op.get_job_node_orchid(first_job_ids[0])}")

        first_job_to_monitoring_descriptors = {job_id: op.get_job_node_orchid(job_id)["monitoring_descriptor"] for job_id in first_job_ids}
        first_allocation_id_to_monitoring_descriptors = {get_allocation_id_from_job_id(job_id): descriptor for job_id, descriptor in first_job_to_monitoring_descriptors.items()}

        if use_operation_id_based_descriptors_for_gangs_jobs:
            check_gang_monitoring_descriptor(op, first_running_jobs, first_job_to_monitoring_descriptors)

        if with_job_revival:
            op.wait_for_fresh_snapshot()
            with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
                pass

            op.wait_for_job_revival_finished()

        # We create it after CA restart to not compare new value of sensor with value before restart.
        restarted_job_profiler = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla", "abort_reason": "operation_incarnation_changed"},
        )

        first_job_id = first_job_ids[0]

        print_debug(f"Old allocation id to monitoring descriptor are: {first_allocation_id_to_monitoring_descriptors}")

        print_debug(f"Aborting job {first_job_id}")

        abort_job(first_job_id)

        wait(lambda: restarted_job_profiler.get_job_count_delta() == 2)

        release_breakpoint(job_id=first_job_ids[0])
        release_breakpoint(job_id=first_job_ids[1])
        release_breakpoint(job_id=first_job_ids[2])

        first_allocation_id_to_monitoring_descriptors.pop(get_allocation_id_from_job_id(first_job_id))

        second_job_ids = wait_breakpoint(job_count=3)
        second_running_jobs = self._get_running_jobs(op, 3)
        self._verify_job_ids_equal(second_running_jobs, second_job_ids)

        assert len(set(first_job_ids) & set(second_job_ids)) == 0

        second_job_to_monitoring_descriptors = {job_id: op.get_job_node_orchid(job_id)["monitoring_descriptor"] for job_id in second_job_ids}
        second_allocation_id_to_monitoring_descriptors = {get_allocation_id_from_job_id(job_id): descriptor for job_id, descriptor in second_job_to_monitoring_descriptors.items()}

        if use_operation_id_based_descriptors_for_gangs_jobs:
            check_gang_monitoring_descriptor(op, second_running_jobs, second_job_to_monitoring_descriptors)

        for allocation_id, profiling_descriptor in first_allocation_id_to_monitoring_descriptors.items():
            assert profiling_descriptor == second_allocation_id_to_monitoring_descriptors[allocation_id]

        release_breakpoint()

        op.track()

    @authors("pogorelov")
    def test_allocation_job_count_reached_limit(self):
        update_controller_agent_config("allocation_job_count_limit", 1)

        restarted_job_profiler = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla", "abort_reason": "operation_incarnation_changed"},
        )
        aborted_by_request_job_profiler = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla", "abort_reason": "user_request"},
        )

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=2,
            task_patch={"gang_options": {}},
        )

        first_incarnation_job_ids = wait_breakpoint(job_count=2)

        assert len(first_incarnation_job_ids) == 2

        first_job_id, second_job_id = first_incarnation_job_ids

        first_incarnation_id = self._get_operation_incarnation(op)

        print_debug(f"First incarnation {first_incarnation_id} jobs are: {first_incarnation_job_ids}")

        print_debug("aborting job ", first_job_id)

        abort_job(first_job_id)

        wait(lambda: aborted_by_request_job_profiler.get_job_count_delta() == 1)
        wait(lambda: restarted_job_profiler.get_job_count_delta() == 1)

        release_breakpoint(job_id=first_job_id)
        release_breakpoint(job_id=second_job_id)

        def get_allocation_ids(job_ids):
            return set([get_allocation_id_from_job_id(job_id) for job_id in job_ids])

        first_incarnation_allocation_ids = get_allocation_ids(first_incarnation_job_ids)

        second_incarnation_job_ids = wait_breakpoint(job_count=2)
        assert len(second_incarnation_job_ids) == 2

        second_incarnation_id = self._get_operation_incarnation(op)

        print_debug(f"Second incarnation {second_incarnation_id} jobs are: {second_incarnation_job_ids}")

        assert first_incarnation_id != second_incarnation_id

        second_incarnation_allocation_ids = get_allocation_ids(second_incarnation_job_ids)

        assert len(first_incarnation_allocation_ids & second_incarnation_allocation_ids) == 0

        release_breakpoint()

        op.track()

    @authors("pogorelov")
    def test_interruption_signal(self):
        restarted_job_profiler = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla", "abort_reason": "operation_incarnation_changed"},
        )

        incarnation_switch_counter = _get_controller_profiler().with_tags({"reason": "job_interrupted"}).counter(
            "controller_agent/gang_operations/incarnation_switch_count")

        exit_code = 17

        command = f"""(trap "exit {exit_code}" SIGINT; BREAKPOINT)"""

        op = run_test_vanilla(
            command=with_breakpoint(command),
            job_count=3,
            task_patch={
                "interruption_signal": "SIGINT",
                "restart_exit_code": exit_code,
                "gang_options": {},
            },
        )
        first_job_ids = wait_breakpoint(job_count=3)
        job_id_to_interrupt = first_job_ids[0]

        print_debug(f"First job ids are {first_job_ids}, interrupting job {job_id_to_interrupt}")

        interrupt_job(job_id_to_interrupt)

        wait(lambda: restarted_job_profiler.get_job_count_delta() == 2)

        assert op.get_job_count("lost") == 1
        # assert op.get_job_count("completed") == 1

        for job_id in first_job_ids:
            release_breakpoint(job_id=job_id)

        wait(lambda: incarnation_switch_counter.get_delta() == 1)

        second_job_ids = wait_breakpoint(job_count=3)

        print_debug(f"Second job ids are {second_job_ids}")

        assert len(set(first_job_ids) & set(second_job_ids)) == 0

        first_allocation_ids = set([get_allocation_id_from_job_id(job_id) for job_id in first_job_ids])
        second_allocation_ids = set([get_allocation_id_from_job_id(job_id) for job_id in second_job_ids])

        assert len(first_allocation_ids & second_allocation_ids) == 2

        release_breakpoint()

        op.track()

    @authors("pogorelov")
    def test_gang_operation_controller_in_failing_state(self):
        update_controller_agent_config("job_tracker/node_disconnection_timeout", 50000)
        update_controller_agent_config("job_tracker/revival_node_disconnection_timeout", 50000)
        update_nodes_dynamic_config(
            path="exec_node/controller_agent_connector/heartbeat_executor",
            value={
                "period": 20000,
                "splay": 0,
            },
        )
        time.sleep(1)

        restarted_job_profiler = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla", "abort_reason": "operation_incarnation_changed"},
        )

        op = vanilla(
            track=False,
            spec={
                "time_limit": 2000,
                "tasks": {
                    "task_a": {
                        "job_count": 2,
                        "command": with_breakpoint("BREAKPOINT"),
                        "gang_options": {},
                    },
                },
            },
        )

        job_ids = wait_breakpoint(job_count=2)
        print_debug(f"Job ids are {job_ids}")

        wait(lambda: get(op.get_path() + "/@progress/state") == "failing")

        job_id_to_abort = job_ids[0]
        print_debug(f"Aborting job {job_id_to_abort}")
        abort_job(job_id_to_abort)

        # User job abort is synchronous.
        update_nodes_dynamic_config(
            path="exec_node/controller_agent_connector/heartbeat_executor/period", value=100)

        op.wait_for_state("failed")

        assert restarted_job_profiler.get_job_count_delta() == 0

    @authors("faucct", "pogorelov")
    def test_gang_operation_with_collective_options(self):
        with pytest.raises(YtError, match='Operation with "collective_options" can not have tasks with "gang_options"'):
            vanilla(
                track=False,
                spec={
                    "tasks": {
                        "task_a": {
                            "job_count": 1,
                            "command": ";",
                            "gang_options": {},
                            "collective_options": {"size": 2},
                        },
                    },
                },
            )

    @authors("pogorelov")
    def test_gang_operation_with_fail_on_job_restart(self):
        with pytest.raises(YtError):
            vanilla(
                track=False,
                spec={
                    "fail_on_job_restart": True,
                    "tasks": {
                        "task_a": {
                            "job_count": 1,
                            "command": ";",
                            "gang_options": {},
                        },
                    },
                },
            )

        with pytest.raises(YtError):
            vanilla(
                track=False,
                spec={
                    "tasks": {
                        "task_a": {
                            "job_count": 1,
                            "command": ";",
                            "gang_options": {},
                            "fail_on_job_restart": True,
                        },
                    },
                },
            )

        with pytest.raises(YtError):
            vanilla(
                track=False,
                spec={
                    "tasks": {
                        "task_a": {
                            "job_count": 1,
                            "command": ";",
                            "gang_options": {},
                        },
                        "task_b": {
                            "job_count": 1,
                            "command": ";",
                            "fail_on_job_restart": True,
                        }
                    },
                },
            )

    @authors("pogorelov")
    def test_gang_operation_with_output_table(self):
        create("table", "//tmp/t")
        with pytest.raises(YtError):
            vanilla(
                spec={
                    "tasks": {
                        "task_a": {
                            "job_count": 1,
                            "output_table_paths": ["//tmp/t"],
                            "format": "yson",
                            "command": "echo '{a=1}'",
                        },
                        "task_b": {
                            "job_count": 1,
                            "gang_options": {},
                            "format": "json",
                            "command": ';',
                        },
                    }
                }
            )

    @authors("pogorelov")
    def test_gang_operation_job_hanging(self):
        # YT-24448
        # In this test we want to switch operation incarnation having postpone finshed allocation event.
        # So we kill one node and increase node_registration_timeout for CA.
        # And after that abort some jobs.
        update_scheduler_config("node_registration_timeout", 500)

        update_controller_agent_config("job_tracker/node_disconnection_timeout", 30000)
        update_controller_agent_config("job_tracker/revival_node_disconnection_timeout", 30000)

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={
                "gang_options": {},
            },
        )

        first_job_ids = wait_breakpoint(job_count=3)
        assert len(first_job_ids) == 3

        first_job_id = first_job_ids[0]
        first_job_allocation_id = get_allocation_id_from_job_id(first_job_id)
        first_job_node = op.get_node(first_job_id)

        print_debug(f"Node address of job {first_job_id} is {first_job_node}")

        op.wait_for_fresh_snapshot()
        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            self.Env.kill_nodes(addresses=[first_job_node])

        # In case of test failure we must start killed node again.
        try:
            # We create it after CA restart to not compare new value of sensor with value before restart.
            incarnation_switch_counter = _get_controller_profiler().counter(
                "controller_agent/gang_operations/incarnation_switch_count")

            second_job_id = first_job_ids[1]
            second_job_node = op.get_node(second_job_id)
            assert first_job_node != second_job_node

            controller_agent_address = get(op.get_path() + "/@controller_agent_address")
            wait(lambda: exists(f"//sys/controller_agents/instances/{controller_agent_address}/orchid/controller_agent/job_tracker/allocations/{first_job_allocation_id}"))

            def _check_allocation_finished():
                allocation_orchid = get(f"//sys/controller_agents/instances/{controller_agent_address}/orchid/controller_agent/job_tracker/allocations/{first_job_allocation_id}")
                if not allocation_orchid["finished"]:
                    return False

                assert allocation_orchid["jobs"][first_job_id]["stage"] == "waiting_for_confirmation"

                return True
            wait(_check_allocation_finished)

            print_debug(f"Aborting job {second_job_id}")

            abort_job(second_job_id)

            # Allocation abort was postponed.
            # After incarnation switching, postponed allocation abort will be processed and it led to new incarnation switch.
            wait(lambda: incarnation_switch_counter.get() >= 1)
        finally:
            self.Env.start_nodes(addresses=[first_job_node])

        release_breakpoint(job_id=first_job_ids[0])
        release_breakpoint(job_id=first_job_ids[1])
        release_breakpoint(job_id=first_job_ids[2])

        wait_breakpoint(job_count=3)

        release_breakpoint()

        op.track()

    @authors("krasovav")
    def test_waiting_for_job_reincarnation_timed_out(self):
        update_controller_agent_config("vanilla_operation_options/gang_manager/job_reincarnation_timeout", 1)

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=2,
            task_patch={
                "gang_options": {},
            },
            spec={
                "job_testing_options": {
                    "delay_in_cleanup": 1000,
                },
            },
        )

        first_job_ids = wait_breakpoint(job_count=2)
        assert len(first_job_ids) == 2

        incarnation_switch_counter = _get_controller_profiler().counter(
            "controller_agent/gang_operations/incarnation_switch_count")
        abort_job(first_job_ids[0])

        wait(lambda: incarnation_switch_counter.get() == 1)

        # Resolve life lock from comment above.
        update_controller_agent_config("vanilla_operation_options/gang_manager/job_reincarnation_timeout", 1000000)

        release_breakpoint(job_id=first_job_ids[0])
        release_breakpoint(job_id=first_job_ids[1])

        release_breakpoint()

        op.track()

    @authors("pogorelov")
    @pytest.mark.parametrize("use_operation_id_based_descriptors_for_gangs_jobs", [False, True])
    def test_gang_operation_monitoring_descriptor_limit(self, use_operation_id_based_descriptors_for_gangs_jobs):
        update_controller_agent_config(path="user_job_monitoring/extended_max_monitored_user_jobs_per_operation", value=1)

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task": {
                        "job_count": 3,  # More jobs than monitoring descriptors available
                        "command": with_breakpoint("BREAKPOINT"),
                        "gang_options": {},
                        "monitoring": {
                            "use_operation_id_based_descriptors_for_gangs_jobs": use_operation_id_based_descriptors_for_gangs_jobs,
                            "enable": True
                        }
                    }
                }
            }
        )

        first_job_ids = wait_breakpoint(job_count=3)
        assert len(first_job_ids) == 3

        wait(lambda: "user_job_monitoring_limited" in op.get_alerts())

        initial_incarnation = self._get_operation_incarnation(op)

        abort_job(first_job_ids[1])

        def check_incarnation_changed():
            current_incarnation = self._get_operation_incarnation(op)
            return current_incarnation != initial_incarnation

        wait(check_incarnation_changed)

        for job_id in first_job_ids:
            release_breakpoint(job_id=job_id)

        second_job_ids = wait_breakpoint(job_count=3)
        assert len(set(first_job_ids) & set(second_job_ids)) == 0
        release_breakpoint()

        op.track()

    @authors("krasovav")
    @pytest.mark.parametrize("use_operation_id_based_descriptors_for_first_task", [True, False])
    @pytest.mark.parametrize("use_operation_id_based_descriptors_for_second_task", [True, False])
    def test_different_monitoing_descriptors_in_operation(self, use_operation_id_based_descriptors_for_first_task, use_operation_id_based_descriptors_for_second_task):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "first": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="first"),
                        "gang_options": {},
                        "monitoring": {
                            "use_operation_id_based_descriptors_for_gangs_jobs": use_operation_id_based_descriptors_for_first_task,
                            "enable": True
                        },
                    },
                    "second": {
                        "job_count": 2,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="second"),
                        "gang_options": {},
                        "monitoring": {
                            "enable": True,
                            "use_operation_id_based_descriptors_for_gangs_jobs": use_operation_id_based_descriptors_for_second_task,
                        }
                    }
                }
            }
        )

        def check_gang_monitoring_descriptor_has_operation_id(op, job_ids, task_index):
            running_jobs = self._get_running_jobs(op, 3)
            job_id_to_rank = self._get_job_id_to_rank_map(running_jobs)
            for job_id in job_ids:
                monitoring_descriptor = op.get_job_node_orchid(job_id)["monitoring_descriptor"]
                expected_job_descriptor = "{}/{}".format(op.id, (job_id_to_rank[job_id] << 2) + task_index)
                assert monitoring_descriptor == expected_job_descriptor

        def check_monitoring_descriptor_hasnt_operation_id(op, job_ids):
            controller_agent_address = get(op.get_path() + "/@controller_agent_address")
            controller_agent_orchid = "//sys/controller_agents/instances/{}/orchid/controller_agent".format(
                controller_agent_address
            )
            incarnation_id = get("{}/incarnation_id".format(controller_agent_orchid))

            expected_monitoring_descriptors = set()
            for i in range(0, 3):
                expected_monitoring_descriptors.add("{}/{}".format(incarnation_id, i))

            for job_id in job_ids:
                monitoring_descriptor = op.get_job_node_orchid(job_id)["monitoring_descriptor"]
                assert monitoring_descriptor in expected_monitoring_descriptors
                expected_monitoring_descriptors.remove(monitoring_descriptor)

        def check_monitoring_descriptor(op, first_task_job_ids, second_task_job_ids):
            if use_operation_id_based_descriptors_for_first_task:
                check_gang_monitoring_descriptor_has_operation_id(op, first_task_job_ids, 0)
            else:
                check_monitoring_descriptor_hasnt_operation_id(op, first_task_job_ids)

            if use_operation_id_based_descriptors_for_second_task:
                check_gang_monitoring_descriptor_has_operation_id(op, second_task_job_ids, 1)
            else:
                check_monitoring_descriptor_hasnt_operation_id(op, second_task_job_ids)

        def check_not_equal_monitoring_descriptor(op, job_ids):
            all_descriptors = set()
            for job_id in job_ids:
                monitoring_descriptor = op.get_job_node_orchid(job_id)["monitoring_descriptor"]
                assert monitoring_descriptor not in all_descriptors
                all_descriptors.add(monitoring_descriptor)

        first_task_job_ids = wait_breakpoint(breakpoint_name="first", job_count=1)
        second_task_job_ids = wait_breakpoint(breakpoint_name="second", job_count=2)
        first_job_ids = first_task_job_ids + second_task_job_ids
        assert len(first_job_ids) == 3

        check_monitoring_descriptor(op, first_task_job_ids, second_task_job_ids)
        check_not_equal_monitoring_descriptor(op, first_job_ids)

        abort_job(first_task_job_ids[0])

        release_breakpoint(job_id=first_task_job_ids[0], breakpoint_name="first")
        release_breakpoint(job_id=second_task_job_ids[0], breakpoint_name="second")
        release_breakpoint(job_id=second_task_job_ids[1], breakpoint_name="second")

        first_task_job_ids = wait_breakpoint(breakpoint_name="first", job_count=1)
        second_task_job_ids = wait_breakpoint(breakpoint_name="second", job_count=2)
        second_job_ids = first_task_job_ids + second_task_job_ids
        assert len(second_job_ids) == 3

        assert second_job_ids != first_job_ids

        check_monitoring_descriptor(op, first_task_job_ids, second_task_job_ids)
        check_not_equal_monitoring_descriptor(op, second_job_ids)

        release_breakpoint(breakpoint_name="first")
        release_breakpoint(breakpoint_name="second")

        op.track()

    @authors("pogorelov")
    def test_gang_size_greater_than_job_count(self):
        with pytest.raises(YtError):
            vanilla(
                spec={
                    "tasks": {
                        "task": {
                            "job_count": 1,
                            "gang_options": {"size": 2},
                        },
                    },
                },
            )

    @authors("pogorelov")
    def test_gang_rank_is_set(self):
        # Die with code 42 if variable is not 0.
        command = '[[ "$YT_GANG_RANK" -eq 0 ]] && [[ "$YT_TASK_GANG_RANK" -eq 0 ]] && exit 0 || exit 42'
        op = vanilla(
            spec={
                "tasks": {
                    "test": {
                        "job_count": 1,
                        "command": command,
                        "gang_options": {},
                    },
                },
                "max_failed_job_count": 1,
            }
        )
        op.track()

    @authors("pogorelov")
    def test_gang_size_is_set(self):
        # Die with code 42 if variable is not 1.
        command = '[[ "$YT_GANG_SIZE" -eq 2 ]] && exit 0 || exit 42'
        op = vanilla(
            spec={
                "tasks": {
                    "a": {
                        "job_count": 1,
                        "command": command,
                        "gang_options": {},
                    },
                    "b": {
                        "job_count": 1,
                        "command": command,
                        "gang_options": {},
                    },
                },
                "max_failed_job_count": 1,
            }
        )
        op.track()

    @authors("pogorelov")
    def test_gang_size_is_set_with_gang_size_in_spec(self):
        # Die with code 42 if variable is not 1.
        command = '[[ "$YT_GANG_SIZE" -eq 2 ]] && exit 0 || exit 42'
        op = vanilla(
            spec={
                "tasks": {
                    "a": {
                        "job_count": 2,
                        "command": command,
                        "gang_options": {"size": 1},
                    },
                    "b": {
                        "job_count": 1,
                        "command": command,
                        "gang_options": {},
                    },
                },
                "max_failed_job_count": 1,
            }
        )
        op.track()

    @authors("pogorelov")
    def test_task_gang_size_is_set(self):
        # Die with code 42 if variable is not 1.
        command = '[[ "$YT_TASK_GANG_SIZE" -eq 1 ]] && exit 0 || exit 42'
        op = vanilla(
            spec={
                "tasks": {
                    "a": {
                        "job_count": 1,
                        "command": command,
                        "gang_options": {},
                    },
                    "b": {
                        "job_count": 1,
                        "command": command,
                        "gang_options": {},
                    },
                },
                "max_failed_job_count": 1,
            }
        )
        op.track()

    @authors("pogorelov")
    @pytest.mark.parametrize("with_revival", [False, True])
    def test_job_rank_distribution(self, with_revival):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={
                "gang_options": {},
            },
        )

        wait_breakpoint(job_count=3)

        running_jobs = self._get_running_jobs(op, 3)

        print_debug(f"Running jobs are {running_jobs}")

        job_ranks = set([info["gang_rank"] for job_id, info in running_jobs.items()])
        assert job_ranks == set(range(3))

        if with_revival:
            op.wait_for_fresh_snapshot()
            with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
                pass
            new_running_jobs = self._get_running_jobs(op, 3)

            for job_id, info in running_jobs.items():
                assert info["gang_rank"] == new_running_jobs[job_id]["gang_rank"], f"New running job infos: {new_running_jobs}"

        release_breakpoint()

        op.track()

    @authors("pogorelov")
    @pytest.mark.parametrize("with_revival", [False, True])
    def test_rank_distribution_with_gang_size_set(self, with_revival):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={"gang_options": {"size": 2}},
        )

        wait_breakpoint(job_count=3)

        running_jobs = self._get_running_jobs(op, 3)

        print_debug(f"Running jobs are {running_jobs}")

        job_ranks = set([info.get("gang_rank") for job_id, info in running_jobs.items()])
        assert job_ranks == set([None, 0, 1])

        if with_revival:
            op.wait_for_fresh_snapshot()
            with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
                pass
            new_running_jobs = self._get_running_jobs(op, 3)

            for job_id, info in running_jobs.items():
                assert info.get("gang_rank") == new_running_jobs[job_id].get("gang_rank"), f"New running job infos: {new_running_jobs}"

        release_breakpoint()

        op.track()

    @authors("pogorelov")
    def test_operation_completes_without_waiting_for_rankless_jobs(self):
        operation_incarnation_counter = _get_controller_profiler().counter("controller_agent/gang_operations/incarnation_switch_count")

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={"gang_options": {"size": 2}},
        )

        job_ids = wait_breakpoint(job_count=3)

        running_jobs = self._get_running_jobs(op, 3)

        print_debug(f"Running jobs are {running_jobs}")

        self._verify_job_ids_equal(running_jobs, job_ids)

        rankless_job_ids = self._get_jobs_with_no_ranks(running_jobs)
        assert len(rankless_job_ids) == 1

        jobs_to_release = [job_id for job_id in running_jobs.keys() if job_id != rankless_job_ids[0]]
        print_debug(f"Releasing jobs with ranks {jobs_to_release}, rankless job will running: {rankless_job_ids}")

        for job_id in jobs_to_release:
            release_breakpoint(job_id=job_id)

        op.track()

        assert operation_incarnation_counter.get_delta() == 0

    @authors("pogorelov")
    def test_job_with_no_rank_abortion_does_not_lead_to_incarnation_switch(self):
        operation_incarnation_counter = _get_controller_profiler().counter("controller_agent/gang_operations/incarnation_switch_count")

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={"gang_options": {"size": 2}},
        )

        initial_incarnation = self._get_operation_incarnation(op)

        job_ids = wait_breakpoint(job_count=3)

        running_jobs = self._get_running_jobs(op, 3)

        self._verify_job_ids_equal(running_jobs, job_ids)

        print_debug(f"Running jobs are {running_jobs}")

        jobs_with_no_ranks = self._get_jobs_with_no_ranks(running_jobs)
        assert len(jobs_with_no_ranks) == 1

        print_debug(f"Aborting job {jobs_with_no_ranks[0]}")
        abort_job(jobs_with_no_ranks[0])
        release_breakpoint(job_id=jobs_with_no_ranks[0])

        new_job_ids = wait_breakpoint(job_count=3)
        new_running_jobs = self._get_running_jobs(op, 3)

        self._verify_job_ids_equal(new_running_jobs, new_job_ids)

        for job_id, info in running_jobs.items():
            if job_id != jobs_with_no_ranks[0]:
                assert info["gang_rank"] == new_running_jobs[job_id]["gang_rank"]

        assert initial_incarnation == self._get_operation_incarnation(op)
        assert operation_incarnation_counter.get_delta() == 0

        release_breakpoint()
        op.track()

    @authors("pogorelov")
    def test_job_with_no_rank_failure_does_not_lead_to_incarnation_switch(self):
        operation_incarnation_counter = _get_controller_profiler().counter("controller_agent/gang_operations/incarnation_switch_count")

        failed_job_profiler = JobCountProfiler(
            "failed",
            tags={"tree": "default", "job_type": "vanilla"},
        )

        command = with_breakpoint("""
            BREAKPOINT;
            if [[ -z "$YT_GANG_RANK" && -z "$YT_TASK_GANG_RANK" ]]; then
                exit 1
            else
                exit 0
            fi;
        """)

        op = run_test_vanilla(
            command=command,
            job_count=3,
            task_patch={
                "gang_options": {"size": 2},
            },
            spec={
                "max_failed_job_count": 10000,
            },
        )

        initial_incarnation = self._get_operation_incarnation(op)

        job_ids = wait_breakpoint(job_count=3)

        running_jobs = self._get_running_jobs(op, 3)

        self._verify_job_ids_equal(running_jobs, job_ids)

        print_debug(f"Running jobs are {running_jobs}")

        jobs_with_no_ranks = self._get_jobs_with_no_ranks(running_jobs)
        assert len(jobs_with_no_ranks) == 1

        print_debug(f"Releasing job with no rank {jobs_with_no_ranks[0]}")
        release_breakpoint(job_id=jobs_with_no_ranks[0])

        wait(lambda: failed_job_profiler.get_job_count_delta() == 1)

        new_job_ids = wait_breakpoint(job_count=3)
        new_running_jobs = self._get_running_jobs(op, 3)

        self._verify_job_ids_equal(new_running_jobs, new_job_ids)

        for job_id, info in running_jobs.items():
            if job_id != jobs_with_no_ranks[0]:
                assert info["gang_rank"] == new_running_jobs[job_id]["gang_rank"]

        assert initial_incarnation == self._get_operation_incarnation(op)
        assert operation_incarnation_counter.get_delta() == 0

        release_breakpoint()
        op.track()

        assert operation_incarnation_counter.get_delta() == 0

    @authors("pogorelov")
    def test_job_with_no_rank_interruption_does_not_lead_to_incarnation_switch(self):
        operation_incarnation_counter = _get_controller_profiler().counter("controller_agent/gang_operations/incarnation_switch_count")

        completed_job_profiler = JobCountProfiler(
            "completed",
            tags={"tree": "default", "job_type": "vanilla"},
        )

        exit_code = 17

        command = f"""(trap "exit {exit_code}" SIGINT; BREAKPOINT)"""

        op = run_test_vanilla(
            command=with_breakpoint(command),
            job_count=3,
            task_patch={
                "interruption_signal": "SIGINT",
                "restart_exit_code": exit_code,
                "gang_options": {"size": 2},
            },
        )

        initial_incarnation = self._get_operation_incarnation(op)

        job_ids = wait_breakpoint(job_count=3)

        running_jobs = self._get_running_jobs(op, 3)

        self._verify_job_ids_equal(running_jobs, job_ids)

        print_debug(f"Running jobs are {running_jobs}")

        jobs_with_no_ranks = self._get_jobs_with_no_ranks(running_jobs)
        assert len(jobs_with_no_ranks) == 1

        print_debug(f"Interrupting job {jobs_with_no_ranks[0]}")
        interrupt_job(jobs_with_no_ranks[0])

        wait(lambda: completed_job_profiler.get_job_count_delta() == 1)

        release_breakpoint(job_id=jobs_with_no_ranks[0])

        new_job_ids = wait_breakpoint(job_count=3)
        new_running_jobs = self._get_running_jobs(op, 3)

        self._verify_job_ids_equal(new_running_jobs, new_job_ids)

        for job_id, info in running_jobs.items():
            if job_id != jobs_with_no_ranks[0]:
                assert info["gang_rank"] == new_running_jobs[job_id]["gang_rank"]

        assert initial_incarnation == self._get_operation_incarnation(op)
        assert operation_incarnation_counter.get_delta() == 0

        release_breakpoint()
        op.track()

        assert operation_incarnation_counter.get_delta() == 0

    @authors("pogorelov")
    def test_rank_reassignment(self):
        operation_incarnation_counter = _get_controller_profiler().counter("controller_agent/gang_operations/incarnation_switch_count")

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={"gang_options": {"size": 2}},
        )

        job_ids = wait_breakpoint(job_count=3)
        running_jobs = self._get_running_jobs(op, 3)
        self._verify_job_ids_equal(running_jobs, job_ids)

        print_debug(f"Running jobs are {running_jobs}")

        job_id_to_rank = self._get_job_id_to_rank_map(running_jobs)
        job_ids_with_no_rank = self._get_jobs_with_no_ranks(running_jobs)
        assert len(job_ids_with_no_rank) == 1
        allocation_id_with_no_rank = get_allocation_id_from_job_id(job_ids_with_no_rank[0])
        job_ids_with_rank = [job_id for job_id in running_jobs.keys() if job_id != job_ids_with_no_rank[0]]
        assert len(job_ids_with_rank) == 2

        print_debug(f"Jobs with rank: {job_ids_with_rank}, job with no rank: {job_ids_with_no_rank[0]}")

        allocation_id_to_rank = self._get_allocation_id_to_rank_map(running_jobs)
        assert len(allocation_id_to_rank) == 3

        aborted_job_rank = job_id_to_rank[job_ids_with_rank[0]]
        print_debug(f"Aborting job {job_ids_with_rank[0]}")
        abort_job(job_ids_with_rank[0])

        wait(lambda: operation_incarnation_counter.get_delta() == 1)

        for job_id in job_ids:
            release_breakpoint(job_id=job_id)

        new_job_ids = wait_breakpoint(job_count=3)
        new_running_jobs = self._get_running_jobs(op, 3)

        print_debug(f"New running jobs are {new_running_jobs}")

        self._verify_job_ids_equal(new_running_jobs, new_job_ids)

        new_allocation_id_to_rank = self._get_allocation_id_to_rank_map(new_running_jobs)
        assert len(new_allocation_id_to_rank) == 3

        # Allocacation A1 that initially has no rank is used for reserved job.
        # When job with rank is aborted, its rank is reassigned to new job of allocation A1.
        assert new_allocation_id_to_rank[allocation_id_with_no_rank] == aborted_job_rank
        assert job_id_to_rank[job_ids_with_rank[1]] == new_allocation_id_to_rank[get_allocation_id_from_job_id(job_ids_with_rank[1])]

        for allocation_id, rank in new_allocation_id_to_rank.items():
            if allocation_id != allocation_id_with_no_rank and allocation_id != get_allocation_id_from_job_id(job_ids_with_rank[1]):
                assert rank is None

        release_breakpoint()
        op.track()

    @authors("pogorelov")
    def test_jobs_with_ranks_scheduled_first(self):
        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/cpu")
        create_pool("test_pool", attributes={"strong_guarantee_resources": {"cpu": total_cpu_limit}})

        sleeping_op = run_sleeping_vanilla(spec={"pool": "test_pool"}, job_count=1)
        wait(lambda: len(get(_get_job_tracker_orchid_path(sleeping_op) + f"/operations/{sleeping_op.id}/allocations")) == 1)

        # Will not start jobs while sleeping_op is running.
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={"gang_options": {"size": 2}},
            spec={"pool": "fake_pool"},
        )

        job_ids = wait_breakpoint(job_count=2)

        running_jobs = self._get_running_jobs(op, 2)
        self._verify_job_ids_equal(running_jobs, job_ids)

        print_debug(f"Running jobs are {running_jobs}")

        job_ids_with_no_ranks = self._get_jobs_with_no_ranks(running_jobs)
        assert len(job_ids_with_no_ranks) == 0

        release_breakpoint()

        op.track()

    @authors("pogorelov")
    def test_revival_without_some_jobs_but_with_all_ranks(self):
        incarnation_switch_counter = _get_controller_profiler().with_tags({"reason": "job_lack_after_revival"}).counter(
            "controller_agent/gang_operations/incarnation_switch_count")

        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/cpu")
        create_pool("test_pool", attributes={"strong_guarantee_resources": {"cpu": total_cpu_limit}})

        sleeping_op = run_sleeping_vanilla(spec={"pool": "test_pool"}, job_count=1)
        wait(lambda: len(get(_get_job_tracker_orchid_path(sleeping_op) + f"/operations/{sleeping_op.id}/allocations")) == 1)

        # Will not start jobs while sleeping_op is running.
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={"gang_options": {"size": 2}},
            spec={"pool": "fake_pool"},
        )

        first_incarnation_id = self._get_operation_incarnation(op)

        wait_breakpoint(job_count=2)

        op.wait_for_fresh_snapshot()

        assert incarnation_switch_counter.get_delta() == 0

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            sleeping_op.abort()

        incarnation_switch_counter = _get_controller_profiler().with_tags({"reason": "job_lack_after_revival"}).counter(
            "controller_agent/gang_operations/incarnation_switch_count")

        second_incarnation_id = self._get_operation_incarnation(op)

        assert first_incarnation_id == second_incarnation_id

        wait_breakpoint(job_count=3)
        release_breakpoint()

        op.track()

        assert incarnation_switch_counter.get() == 0


##################################################################


class TestPatchVanillaSpecBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 1000,
        },
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 20,
                "user_slots": 20,
                "memory": 2 ** 32,
            },
        },
    }

    def _run_vanilla(self, tasks=["task"], task_spec_template={}, spec_template={}):
        task_spec = deepcopy(task_spec_template)
        task_spec.update({
            "job_count": 2,
            "command": with_breakpoint("BREAKPOINT ; true"),
        })
        spec = deepcopy(spec_template)
        spec.update({
            "tasks": {
                task: task_spec
                for task in tasks
            },
        })
        return vanilla(
            track=False,
            spec=spec
        )

    def assert_job_states(
        self,
        op,
        task_name,
        running=None,
        completed=None,
        aborted=None,
        invalidated=None,
    ):
        states = self._get_job_counters(op, task_name)

        if running is not None:
            assert states["running"] == running, f"States are: {states}"
        if completed is not None:
            assert states["completed"] == completed, f"States are: {states}"
        if aborted is not None:
            assert states["aborted"] == aborted, f"States are: {states}"
        if invalidated is not None:
            assert states["invalidated"] == invalidated, f"States are: {states}"

    def _get_job_counters(self, op, task_name):
        tasks_progress = get(op.get_path() + "/controller_orchid/progress/tasks", verbose=True)
        states = {}
        for task in tasks_progress:
            if task["task_name"] != task_name:
                continue
            counter = task["job_counter"]
            states["running"] = counter["running"]
            states["invalidated"] = counter["invalidated"]
            states["aborted"] = counter["aborted"]["total"]
            states["completed"] = counter["completed"]["total"]
            states["total"] = counter["total"]

        print_debug(f"Task '{task_name}' states: {states}")
        return states

    def _set_job_count(self, op, count, task="task", sync=True):
        return execute_command(
            "patch_op_spec",
            {
                "operation_id": op.id,
                "patches": [
                    {"path": f"/tasks/{task}/job_count", "value": count},
                ],
            },
            return_response=not sync,
        )


class TestPatchVanillaSpec(TestPatchVanillaSpecBase):
    @authors("coteeq")
    def test_increase_job_count(self):
        op = self._run_vanilla()

        wait_breakpoint(job_count=2)

        self._set_job_count(op, 4)

        wait_breakpoint(job_count=4)
        release_breakpoint()
        op.track()

        self.assert_job_states(op, "task", completed=4)

    @authors("coteeq")
    def test_decrease_job_count(self):
        op = self._run_vanilla()

        wait_breakpoint(job_count=2)

        self._set_job_count(op, 1)

        wait(lambda: len(op.get_running_jobs()) == 1)

        release_breakpoint()
        op.track()

        self.assert_job_states(op, "task", aborted=1, completed=1)

    @authors("coteeq")
    def test_regenerate_jobs(self):
        op = self._run_vanilla()

        wait_breakpoint(job_count=2)

        self._set_job_count(op, 1)
        wait(lambda: len(op.get_running_jobs()) == 1)
        self.assert_job_states(op, "task", aborted=1, running=1)

        self._set_job_count(op, 2)
        wait(lambda: len(op.get_running_jobs()) == 2)
        self.assert_job_states(op, "task", aborted=1, running=2)

        release_breakpoint()
        op.track()

        self.assert_job_states(op, "task", aborted=1, completed=2)

    @authors("coteeq")
    def test_regenerate_jobs_and_restart_completed(self):
        op = self._run_vanilla(task_spec_template={"restart_completed_jobs": True})

        wait_breakpoint(job_count=2)

        self._set_job_count(op, 1)
        wait(lambda: len(op.get_running_jobs()) == 1)
        self.assert_job_states(op, "task", aborted=1, running=1)

        self._set_job_count(op, 2)
        wait(lambda: len(op.get_running_jobs()) == 2)
        self.assert_job_states(op, "task", aborted=1, running=2)

        def wait_jobs(release: bool):
            wait(lambda: len(op.get_running_jobs()) == 2)
            old_jobs = op.get_running_jobs()
            for job_id in old_jobs:
                wait_breakpoint(job_id=job_id)
            if release:
                for job_id in old_jobs:
                    release_breakpoint(job_id=job_id)

            return old_jobs

        # Wait until all expected jobs are started and complete (aka release) them.
        old_jobs = wait_jobs(release=True)

        # Wait until jobs are restarted from the controller's point of view.
        wait(lambda: builtins.set(op.get_running_jobs()).isdisjoint(builtins.set(old_jobs)))
        # And from the jobs' point of view.
        wait_jobs(release=False)

        self.assert_job_states(op, "task", aborted=1, completed=0, running=2)

    @authors("coteeq")
    def test_stress_pendulum(self):
        op = self._run_vanilla(tasks=["one", "two"])

        wait_breakpoint(job_count=2 + 2)

        patch_op_spec(
            op.id,
            patches=[
                {"path": "/tasks/one/job_count", "value": 5},
                {"path": "/tasks/two/job_count", "value": 5},
            ]
        )

        wait(lambda: len(op.get_running_jobs()) == 5 + 5)
        one_job_counts = [4, 6, 3, 7, 2, 8, 1, 9]
        two_job_counts = list(reversed(one_job_counts))

        prev_one_job_count = 5
        prev_two_job_count = 5
        total_one_aborted = 0
        total_two_aborted = 0
        for one_job_count, two_job_count in zip(one_job_counts, two_job_counts):
            patch_op_spec(
                op.id,
                patches=[
                    {"path": "/tasks/one/job_count", "value": one_job_count},
                    {"path": "/tasks/two/job_count", "value": two_job_count},
                ]
            )

            def are_new_jobs_alive():
                return all([
                    self._get_job_counters(op, "one")["running"] == one_job_count,
                    self._get_job_counters(op, "two")["running"] == two_job_count,
                ])

            wait(are_new_jobs_alive)

            if prev_one_job_count > one_job_count:
                total_one_aborted += prev_one_job_count - one_job_count
            if prev_two_job_count > two_job_count:
                total_two_aborted += prev_two_job_count - two_job_count

            self.assert_job_states(op, "one", aborted=total_one_aborted, running=one_job_count)
            self.assert_job_states(op, "two", aborted=total_two_aborted, running=two_job_count)

            prev_one_job_count = one_job_count
            prev_two_job_count = two_job_count

        release_breakpoint()
        op.track()

        self.assert_job_states(op, "one", aborted=total_one_aborted, completed=prev_one_job_count)
        self.assert_job_states(op, "two", aborted=total_two_aborted, completed=prev_two_job_count)

    @authors("coteeq")
    def test_abort_pending(self):
        op = self._run_vanilla(spec_template={"resource_limits": {"user_slots": 1}})

        time.sleep(2)
        self.assert_job_states(op, "task", running=1)

        wait_breakpoint(job_count=1)
        self._set_job_count(op, 1)

        # Wait for anything bad to happen.
        time.sleep(2)

        self.assert_job_states(op, "task", aborted=0, running=1, completed=0)

        release_breakpoint()
        op.track()
        self.assert_job_states(op, "task", aborted=0, running=0, completed=1)

    @authors("coteeq")
    def test_forbidden_cases(self):
        def check(**kwargs):
            op = self._run_vanilla(**kwargs)
            op.wait_for_state("running")

            with raises_yt_error("Cannot update"):
                self._set_job_count(op, 4)

        check(spec_template={"fail_on_job_restart": True})
        check(task_spec_template={"fail_on_job_restart": True})
        check(task_spec_template={"gang_options": {}})

        op = self._run_vanilla()
        op.wait_for_state("running")
        with raises_yt_error("Validation failed at /tasks/task/job_count"):
            self._set_job_count(op, 0)

    @authors("coteeq")
    def test_operation_finishes_prematurely(self):
        op = self._run_vanilla()

        wait_breakpoint(job_count=2)

        second_job = [
            job_id
            for job_id, attributes in op.get_running_jobs().items()
            if attributes["job_cookie"] == 1
        ][0]

        release_breakpoint(job_id=second_job)
        wait(lambda: len(op.get_running_jobs()) == 1)
        self._set_job_count(op, 1)

        op.track()

        self.assert_job_states(op, "task", aborted=1, completed=1, invalidated=1)


class TestPatchVanillaSpecRestarts(TestPatchVanillaSpecBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts.

    def _restart_controller(self):
        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

    def _run_vanilla(self, *args, delay_in_apply, spec_template={}, **kwargs):
        spec_template.update({
            "testing": {
                "patch_spec_protocol": {
                    "delay_inside_apply": {
                        "duration": delay_in_apply
                    }
                }
            }
        })
        return super()._run_vanilla(*args, spec_template=spec_template, **kwargs)

    def _assert_no_clean_start(self, op):
        events = get(op.get_path() + "/@events")
        assert len([event for event in events if event["state"] == "initializing"]) == 1

    @authors("coteeq")
    def test_increase_job_count(self):
        op = self._run_vanilla(delay_in_apply="3s")

        wait_breakpoint(job_count=2)
        op.wait_for_fresh_snapshot()

        response = self._set_job_count(op, 4, sync=False)
        time.sleep(1)
        self._restart_controller()

        op.wait_for_state("running")
        wait(lambda: len(op.get_running_jobs()) == 4)
        self.assert_job_states(op, "task", running=4, aborted=0, completed=0, invalidated=0)

        self._assert_no_clean_start(op)

        release_breakpoint()
        op.track()

        self.assert_job_states(op, "task", completed=4, aborted=0)

        response.wait()
        assert not response.is_ok()

    @authors("coteeq")
    def test_decrease_job_count(self):
        op = self._run_vanilla(delay_in_apply="3s")

        wait_breakpoint(job_count=2)
        op.wait_for_fresh_snapshot()

        response = self._set_job_count(op, 1, sync=False)
        time.sleep(1)
        self._restart_controller()

        wait(lambda: len(op.get_running_jobs(verbose=True)) == 1)
        self.assert_job_states(op, "task", running=1, aborted=1, completed=0, invalidated=1)

        self._assert_no_clean_start(op)

        release_breakpoint()
        op.track()

        self.assert_job_states(op, "task", aborted=1, completed=1)

        response.wait()
        assert not response.is_ok()

    @authors("coteeq")
    def test_clean_start(self):
        update_controller_agent_config("enable_snapshot_building", False)
        op = self._run_vanilla(delay_in_apply=0)

        wait_breakpoint(job_count=2)

        self._set_job_count(op, 3)
        time.sleep(1)
        self._restart_controller()

        wait(lambda: len(op.get_running_jobs(verbose=True)) == 3)
        self.assert_job_states(op, "task", running=3, aborted=0)

        release_breakpoint()
        op.track()

        self.assert_job_states(op, "task", aborted=0, completed=3)


##################################################################

class TestDontStartNewIncarnationAfterPreemptionIfJobNotStartedYet(YTEnvSetup):
    ENABLE_MULTIDAEMON = False
    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 1,
            },
        },
    }

    @authors("krasovav")
    def test_dont_start_new_incarnation_after_preemption_if_job_not_started_yet(self):
        create_pool("without_guarantee")
        create_pool("with_guarantee", attributes={"strong_guarantee_resources": {"cpu": 2}})

        operation_incarnation_counter = _get_controller_profiler().counter("controller_agent/gang_operations/incarnation_switch_count")
        op1 = run_test_vanilla(
            with_breakpoint("BREAKPOINT", breakpoint_name="first"),
            job_count=2,
            task_patch={
                "gang_options": {},
            },
            spec={
                "pool": "without_guarantee",
                "is_gang": True,
                "testing": {
                    "settle_job_delay": {
                        "duration": 2000,
                    },
                },
            },
        )

        wait_breakpoint(breakpoint_name="first", job_count=2)

        run_test_vanilla(
            with_breakpoint("BREAKPOINT", breakpoint_name="second"),
            job_count=2,
            spec={
                "pool": "with_guarantee",
            }
        )

        wait_breakpoint(breakpoint_name="second", job_count=2)

        wait(lambda: operation_incarnation_counter.get_delta() == 1)
        wait(lambda: op1.get_job_count("aborted") == 3)
