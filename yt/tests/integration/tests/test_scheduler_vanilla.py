import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

from yt.yson import to_yson_type

import datetime

from collections import Counter

##################################################################

class TestSchedulerVanillaCommands(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 200,
        }
    }

    def test_simple(self):
        command = " ; ".join([
            events_on_fs().notify_event_cmd("job_started_${YT_JOB_INDEX}"),
            events_on_fs().wait_event_cmd("finish")
        ])
        op = vanilla(
            dont_track=True,
            spec={
                "tasks": {
                    "master": {
                        "job_count": 1,
                        "command": command,
                    },
                    "slave": {
                        "job_count": 2,
                        "command": command,
                    },
                },
            })

        # Ensure that all three jobs have started.
        events_on_fs().wait_event("job_started_0", timeout=datetime.timedelta(1000))
        events_on_fs().wait_event("job_started_1", timeout=datetime.timedelta(1000))
        events_on_fs().wait_event("job_started_2", timeout=datetime.timedelta(1000))

        events_on_fs().notify_event("finish")

        op.track()

        get("//sys/operations/{0}/@progress/data_flow_graph".format(op.id))
        assert get("//sys/operations/{0}/@progress/data_flow_graph/vertices/master/job_type".format(op.id)) == "vanilla"
        assert get("//sys/operations/{0}/@progress/data_flow_graph/vertices/master/job_counter/completed/total".format(op.id)) == 1
        assert get("//sys/operations/{0}/@progress/data_flow_graph/vertices/slave/job_type".format(op.id)) ==  "vanilla"
        assert get("//sys/operations/{0}/@progress/data_flow_graph/vertices/slave/job_counter/completed/total".format(op.id)) == 2

    def test_task_job_index(self):
        master_command = " ; ".join([
            events_on_fs().notify_event_cmd("job_started_master_${YT_TASK_JOB_INDEX}"),
            events_on_fs().wait_event_cmd("finish")
        ])

        slave_command = " ; ".join([
            events_on_fs().notify_event_cmd("job_started_slave_${YT_TASK_JOB_INDEX}"),
            events_on_fs().wait_event_cmd("finish")
        ])

        op = vanilla(
            dont_track=True,
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
            })

        # Ensure that all three jobs have started.
        events_on_fs().wait_event("job_started_master_0", timeout=datetime.timedelta(1000))
        events_on_fs().wait_event("job_started_slave_0", timeout=datetime.timedelta(1000))
        events_on_fs().wait_event("job_started_slave_1", timeout=datetime.timedelta(1000))
        events_on_fs().wait_event("job_started_slave_2", timeout=datetime.timedelta(1000))

        events_on_fs().notify_event("finish")

        op.track()

        get("//sys/operations/{0}/@progress/data_flow_graph".format(op.id))
        assert get("//sys/operations/{0}/@progress/data_flow_graph/vertices/master/job_type".format(op.id)) == "vanilla"
        assert get("//sys/operations/{0}/@progress/data_flow_graph/vertices/master/job_counter/completed/total".format(op.id)) == 1
        assert get("//sys/operations/{0}/@progress/data_flow_graph/vertices/slave/job_type".format(op.id)) ==  "vanilla"
        assert get("//sys/operations/{0}/@progress/data_flow_graph/vertices/slave/job_counter/completed/total".format(op.id)) == 3

    def test_files(self):
        create("file", "//tmp/a")
        write_file("//tmp/a", "data_a")
        create("file", "//tmp/b")
        write_file("//tmp/b", "data_b")

        vanilla(
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 1,
                        "command": 'if [[ `cat data` != "data_a" ]] ; then exit 1; fi',
                        "file_paths": [to_yson_type("//tmp/a", attributes={"file_name": "data"})]
                    },
                    "task_b": {
                        "job_count": 2,
                        "command": 'if [[ `cat data` != "data_b" ]] ; then exit 1; fi',
                        "file_paths": [to_yson_type("//tmp/b", attributes={"file_name": "data"})]
                    },
                },
                "max_failed_job_count": 1,
            })

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
                "stderr_table_path": "//tmp/stderr"
            })

        table_stderrs = read_table("//tmp/stderr")
        table_stderrs_per_task = Counter(row["data"] for row in table_stderrs)

        job_ids = ls("//sys/operations/{0}/jobs".format(op.id))
        cypress_stderrs_per_task = Counter(read_file("//sys/operations/{0}/jobs/{1}/stderr".format(op.id, job_id)) for job_id in job_ids)

        assert dict(table_stderrs_per_task) == {"task_a\n": 3, "task_b\n": 2}
        assert dict(cypress_stderrs_per_task) == {"task_a\n": 3, "task_b\n": 2}

    def test_fail_on_failed_job(self):
        with pytest.raises(YtError):
            op = vanilla(
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
                })

    def test_revival_with_fail_on_job_restart(self):
        op = vanilla(
            dont_track=True,
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
            })
        # By this moment all 2 running jobs made it to snapshot, so operation will not fail on revival.
        time.sleep(1.0)
        self.Env.kill_schedulers()
        self.Env.start_schedulers()
        release_breakpoint()
        op.track()

        with pytest.raises(YtError):
            op = vanilla(
                dont_track=True,
                spec={
                    "tasks": {
                        "task_a": {
                            "job_count": 1,
                            "command": "", # do nothing
                        },
                        "task_b": {
                            "job_count": 6,
                            "command": with_breakpoint("BREAKPOINT"),
                        },
                    },
                    "fail_on_job_restart": True,
                })
            # 6 jobs may not be running simultaneously, so the snapshot will contain information about
            # at most 5 running jobs plus 1 completed job, leading to operation fail on revival.
            time.sleep(1.0)
            self.Env.kill_schedulers()
            self.Env.start_schedulers()
            op.track()

    def test_abandon_job(self):
        # Abandoning vanilla job is ok.
        op = vanilla(
            dont_track=True,
            spec={
                "tasks": {
                    "tasks_a": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT ; exit 0"),
                    }
                },
                "fail_on_job_restart": True
            })
        job_id = wait_breakpoint()[0]
        jobs = ls("//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op.id))
        assert len(jobs) == 1
        abandon_job(job_id)
        release_breakpoint()
        op.track()

    def test_non_interruptible(self):
        op = vanilla(
            dont_track=True,
            spec={
                "tasks": {
                    "tasks_a": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT ; exit 0"),
                    }
                },
                "fail_on_job_restart": True
            })
        wait_breakpoint()
        jobs = ls("//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op.id))
        assert len(jobs) == 1
        job_id = jobs[0]
        with pytest.raises(YtError):
            interrupt_job(job_id)

    # TODO(max42): add lambda job: signal_job(job, "SIGKILL") when YT-8243 is fixed.
    @pytest.mark.parametrize("action", [abort_job])
    def test_fail_on_manually_stopped_job(self, action):
        with pytest.raises(YtError):
            op = vanilla(
                dont_track=True,
                spec={
                    "tasks": {
                        "task_a": {
                            "job_count": 1,
                            "command": " ; ".join([events_on_fs().notify_event_cmd("job_started_a"), events_on_fs().wait_event_cmd("finish_a")]),
                        },
                        "task_b": {
                            "job_count": 1,
                            "command": " ; ".join([events_on_fs().notify_event_cmd("job_started_b"), events_on_fs().wait_event_cmd("finish_b")]),
                        },
                    },
                    "fail_on_job_restart": True,
                })
            events_on_fs().wait_event("job_started_a")
            events_on_fs().wait_event("job_started_b")
            jobs = ls("//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op.id))
            assert len(jobs) == 2
            job_id = jobs[0]
            action(job_id)
            events_on_fs().notify_event("finish_a")
            events_on_fs().notify_event("finish_b")
            op.track()

##################################################################

class TestSchedulerVanillaCommandsMulticell(TestSchedulerVanillaCommands):
    UM_SECONDARY_MASTER_CELLS = 2
