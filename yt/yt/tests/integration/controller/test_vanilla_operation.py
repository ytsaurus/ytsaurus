from yt_env_setup import YTEnvSetup, Restarter, SCHEDULERS_SERVICE, CONTROLLER_AGENTS_SERVICE

from yt_commands import (
    authors, print_debug, wait, wait_breakpoint, release_breakpoint, with_breakpoint, events_on_fs,
    raises_yt_error, update_controller_agent_config, update_nodes_dynamic_config,
    create, ls, exists, sorted_dicts, create_pool,
    get, write_file, read_table, write_table, vanilla, run_test_vanilla, abort_job, abandon_job,
    interrupt_job, dump_job_context, run_sleeping_vanilla, get_allocation_id_from_job_id)

from yt_helpers import skip_if_no_descending, profiler_factory, read_structured_log, write_log_barrier, JobCountProfiler

from yt import yson
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

    @authors("arkady-e1ppa")
    def test_operation_incarnation_is_set(self):
        if self.Env.get_component_version("ytserver-controller-agent").abi <= (24, 1):
            pytest.skip()

        # NB(arkady-e1ppa): Die with code 42 if variable is not set.
        command = '[[ -z "$YT_OPERATION_INCARNATION" ]] && exit 42 || exit 0'
        op = vanilla(
            spec={
                "tasks": {
                    "test": {
                        "job_count": 1,
                        "command": command,
                    },
                },
                "fail_on_job_restart": True,
            }
        )
        op.wait_for_state("completed")
        op.track()


class TestYTDiscoveryServiceInVanilla(YTEnvSetup):
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


##################################################################


class TestGangManager(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "gang_manager": {
                "enabled": True,
            },

            "snapshot_period": 1000,
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

    def _get_job_tracker_orchid_path(self, op):
        controller_agent_address = op.get_controller_agent_address()
        return f"//sys/controller_agents/instances/{controller_agent_address}/orchid/controller_agent/job_tracker"

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

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={"gang_manager": {}},
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

        assert len(set(job_ids) & set(new_job_ids)) == 0

        allocation_ids = set([get_allocation_id_from_job_id(job_id) for job_id in job_ids])
        allocation_ids.remove(get_allocation_id_from_job_id(first_job_id))
        new_allocation_ids = set([get_allocation_id_from_job_id(job_id) for job_id in new_job_ids])

        assert allocation_ids.issubset(new_allocation_ids)

        release_breakpoint()

        op.track()

    @authors("pogorelov")
    def test_restart_disabled_in_config(self):
        update_controller_agent_config("vanilla_operation_options/gang_manager/enabled", False)

        restarted_job_profiler = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla"},
        )
        aborted_by_request_job_profiler = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla", "abort_reason": "user_request"},
        )

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={"gang_manager": {}},
        )

        job_ids = wait_breakpoint(job_count=3)

        assert len(job_ids) == 3

        first_job_id = job_ids[0]

        print_debug("aborting job ", first_job_id)

        abort_job(first_job_id)

        wait(lambda: aborted_by_request_job_profiler.get_job_count_delta() == 1)

        release_breakpoint()

        op.track()

        assert restarted_job_profiler.get_job_count_delta() == 1

    @authors("pogorelov")
    def test_restart_on_job_failure(self):
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
            task_patch={"gang_manager": {}},
        )

        job_ids = wait_breakpoint(job_count=3)

        assert len(job_ids) == 3

        job_orchid_addresses = [op.get_job_node_orchid_path(job_id) + f"/exec_node/job_controller/active_jobs/{job_id}" for job_id in job_ids]

        release_breakpoint(job_id=job_ids[0])
        release_breakpoint(job_id=job_ids[1])
        release_breakpoint(job_id=job_ids[2])

        wait(lambda: failed_job_profiler.get_job_count_delta() == 1)
        wait(lambda: restarted_job_profiler.get_job_count_delta() == 2)

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
                        "gang_manager": {},
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
        else:
            assert aborted_job_profiler.get_job_count_delta() == 1
            assert restarted_job_profiler.get_job_count_delta() == 0

    @authors("pogorelov")
    def test_multiple_incarnation_switches(self):
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
            task_patch={"gang_manager": {}},
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
    def test_simple_revive(self):
        aborted_job_profiler = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla"},
        )

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
                        "gang_manager": {},
                    },
                },
                "fail_on_job_restart": False,
            },
        )

        (job_a, ) = wait_breakpoint(breakpoint_name="task_a")
        (job_b, ) = wait_breakpoint(breakpoint_name="task_b")

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        release_breakpoint(breakpoint_name="task_a")
        release_breakpoint(breakpoint_name="task_b")

        op.track()

        assert aborted_job_profiler.get_job_count_delta() == 0

    @authors("pogorelov")
    @pytest.mark.parametrize("jobs_were_scheduled", [0, 1])
    def test_revive_before_jobs_scheduled(self, jobs_were_scheduled):
        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/cpu")
        create_pool("test_pool", attributes={"min_share_resources": {"cpu": total_cpu_limit}})

        sleeping_op = run_sleeping_vanilla(spec={"pool": "test_pool"}, job_count=(3 - jobs_were_scheduled))
        wait(lambda: len(get(self._get_job_tracker_orchid_path(sleeping_op) + f"/operations/{sleeping_op.id}/allocations")) == 3 - jobs_were_scheduled)

        # Will not start jobs while sleeping_op is running.
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={"gang_manager": {}},
            spec={"pool": "fake_pool"},
        )

        first_incarnation_id = self._get_operation_incarnation(op)

        if jobs_were_scheduled:
            wait(lambda: len(get(self._get_job_tracker_orchid_path(op) + f"/operations/{op.id}/allocations")) == jobs_were_scheduled)
        else:
            assert len(get(self._get_job_tracker_orchid_path(op) + f"/operations/{op.id}/allocations")) == 0

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            sleeping_op.abort()

        second_incarnation_id = self._get_operation_incarnation(op)

        assert first_incarnation_id != second_incarnation_id

        wait_breakpoint(job_count=3)
        release_breakpoint()

        op.track()

    @authors("pogorelov")
    def test_restart_completed_jobs(self):
        completed_job_profiler = JobCountProfiler(
            "completed",
            tags={"tree": "default", "job_type": "vanilla"},
        )

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={"gang_manager": {}},
        )

        job_ids = wait_breakpoint(job_count=3)

        print_debug("Running jobs {}; Completing {} and {}, restarting {}".format(job_ids, job_ids[0], job_ids[1], job_ids[2]))

        release_breakpoint(job_id=job_ids[0])
        release_breakpoint(job_id=job_ids[1])

        wait(lambda: completed_job_profiler.get_job_count_delta() == 2)

        abort_job(job_ids[2])
        release_breakpoint(job_id=job_ids[2])

        # wait(lambda: restarted_job_profiler.get_job_count_delta() == 2)

        job_ids = wait_breakpoint(job_count=3)
        release_breakpoint()
        op.track()

    @authors("pogorelov")
    @pytest.mark.parametrize("with_job_revival", [False, True])
    def test_preserving_job_cookie_for_allocation(self, with_job_revival):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={"gang_manager": {}},
        )

        job_ids = wait_breakpoint(job_count=3)
        assert len(job_ids) == 3

        if with_job_revival:
            op.wait_for_fresh_snapshot()
            with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
                pass

            # Waiting for operation revival
            check_path = op.get_path() + "/controller_orchid/progress/jobs"
            wait(lambda: exists(check_path))

        # We create it after CA restart to not compare new value of sensor with value before restart.
        restarted_job_profiler = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla", "abort_reason": "operation_incarnation_changed"},
        )

        first_job_id = job_ids[0]

        def get_running_jobs():
            running_jobs = []

            def check():
                nonlocal running_jobs
                running_jobs = op.get_running_jobs()
                return len(running_jobs) == 3

            wait(check)
            return running_jobs

        running_jobs = get_running_jobs()
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

        running_jobs = get_running_jobs()
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
            task_patch={"gang_manager": {}},
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

        exit_code = 17
        command = f"""(trap "exit {exit_code}" SIGINT; BREAKPOINT)"""

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "tasks_a": {
                        "job_count": 3,
                        "command": with_breakpoint(command),
                        "interruption_signal": "SIGINT",
                        "restart_exit_code": exit_code,
                        "gang_manager": {},
                    }
                },
            },
        )
        first_job_ids = wait_breakpoint(job_count=3)
        job_id_to_interrupt = first_job_ids[0]

        print_debug(f"First job ids are {first_job_ids}, interrupting job {job_id_to_interrupt}")

        try:
            interrupt_job(job_id_to_interrupt, interrupt_timeout=600000)
        except YtError as e:
            # Sometimes job proxy may finish before it manages to send Interrupt reply.
            # This is not an error.
            socket_was_closed_error_code = 100
            assert e.contains_code(socket_was_closed_error_code)

        wait(lambda: restarted_job_profiler.get_job_count_delta() == 2)

        assert op.get_job_count("lost") == 1
        # assert op.get_job_count("completed") == 1

        for job_id in first_job_ids:
            release_breakpoint(job_id=job_id)

        second_job_ids = wait_breakpoint(job_count=3)

        print_debug(f"Second job ids are {second_job_ids}")

        assert len(set(first_job_ids) & set(second_job_ids)) == 0

        first_allocation_ids = set([get_allocation_id_from_job_id(job_id) for job_id in first_job_ids])
        second_allocation_ids = set([get_allocation_id_from_job_id(job_id) for job_id in second_job_ids])

        assert len(first_allocation_ids & second_allocation_ids) == 2

        release_breakpoint()

        op.track()

    @authors("pogorelov")
    def test_gang_manager_with_controller_in_failing_state(self):
        update_controller_agent_config("job_tracker/enable_graceful_abort", True)
        update_controller_agent_config("job_tracker/node_disconnection_timeout", 50000)
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
                        "gang_manager": {},
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

    @authors("pogorelov")
    def test_gang_manager_with_fail_on_job_restart(self):
        with pytest.raises(YtError):
            vanilla(
                track=False,
                spec={
                    "fail_on_job_restart": True,
                    "tasks": {
                        "task_a": {
                            "job_count": 1,
                            "command": ";",
                            "gang_manager": {},
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
                            "gang_manager": {},
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
                            "gang_manager": {},
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
    def test_gang_manager_with_output_table(self):
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
                            "gang_manager": {},
                            "format": "json",
                            "command": ';',
                        },
                    }
                }
            )


##################################################################
