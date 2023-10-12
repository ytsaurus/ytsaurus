from yt_env_setup import YTEnvSetup, Restarter, SCHEDULERS_SERVICE

from yt_commands import (
    authors, print_debug, wait, wait_breakpoint, release_breakpoint, with_breakpoint, events_on_fs,
    create, ls, sorted_dicts,
    get, write_file, read_table, write_table, vanilla, run_test_vanilla, abort_job, abandon_job,
    interrupt_job, dump_job_context)

from yt_helpers import skip_if_no_descending, profiler_factory, read_structured_log, write_log_barrier
from yt.yson import to_yson_type
from yt.common import YtError, date_string_to_datetime

import pytest
from flaky import flaky

import datetime
import time
from collections import Counter

##################################################################


class TestSchedulerVanillaCommands(YTEnvSetup):
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

    @authors("max42")
    def test_revival_with_fail_on_job_restart(self):
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

        with pytest.raises(YtError):
            op = vanilla(
                track=False,
                spec={
                    "tasks": {
                        "task_a": {
                            "job_count": 1,
                            "command": "",  # do nothing
                        },
                        "task_b": {
                            "job_count": 6,
                            "command": with_breakpoint("BREAKPOINT"),
                        },
                    },
                    "fail_on_job_restart": True,
                },
            )
            # 6 jobs may not be running simultaneously, so the snapshot will contain information about
            # at most 5 running jobs plus 1 completed job, leading to operation fail on revival.
            op.wait_for_fresh_snapshot()
            with Restarter(self.Env, SCHEDULERS_SERVICE):
                pass
            op.track()

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
        skip_if_no_descending(self.Env)

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
        # NB: scheduler can manage new scheduled job while CA is processing job abort.
        # So number of aborted jobs can be equal to 3.
        assert len(aborted_job_events) >= 2
        assert aborted_job_events[job_id]["reason"] == "user_request"
        assert aborted_job_events[other_job_id]["reason"] == "operation_failed"
        assert now - date_string_to_datetime(aborted_job_events[other_job_id]["finish_time"]) < datetime.timedelta(minutes=1)

    @authors("coteeq")
    def test_duplicate_output_tables(self):
        if self.Env.get_component_version("ytserver-controller-agent").abi <= (23, 2):
            pytest.skip()

        try:
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
        except YtError as e:
            assert "Duplicate entries in output_table_paths" in str(e)
        else:
            assert False, "operation should've failed"


class TestSchedulerVanillaCommandsMulticell(TestSchedulerVanillaCommands):
    NUM_SECONDARY_MASTER_CELLS = 2


##################################################################

class TestSchedulerVanillaInterrupts(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    def _interrupt(self, op):
        jobs = list(op.get_running_jobs())
        assert len(jobs) == 1
        job_id = jobs[0]
        try:
            interrupt_job(job_id, interrupt_timeout=600000)
        except YtError as e:
            # Sometimes job proxy may finish before it manages to send Interrupt reply.
            # This is not an error.
            socket_was_closed_error_code = 100
            assert e.contains_code(socket_was_closed_error_code)
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


class TestSchedulerVanillaInterruptsPorto(TestSchedulerVanillaInterrupts):
    USE_PORTO = True
