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

    def setup(self):
        self.events = EventsOnFs()

    def test_simple(self):
        command = " ; ".join([
            self.events.notify_event_cmd("job_started_${YT_JOB_INDEX}"),
            self.events.wait_event_cmd("finish")
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
        self.events.wait_event("job_started_0", timeout=datetime.timedelta(1000))
        self.events.wait_event("job_started_1", timeout=datetime.timedelta(1000))
        self.events.wait_event("job_started_2", timeout=datetime.timedelta(1000))

        self.events.notify_event("finish")

        op.track()

        get("//sys/operations/{0}/@progress/data_flow_graph".format(op.id))
        assert get("//sys/operations/{0}/@progress/data_flow_graph/vertices/master/job_type".format(op.id)) == "vanilla"
        assert get("//sys/operations/{0}/@progress/data_flow_graph/vertices/master/job_counter/completed/total".format(op.id)) == 1
        assert get("//sys/operations/{0}/@progress/data_flow_graph/vertices/slave/job_type".format(op.id)) ==  "vanilla"
        assert get("//sys/operations/{0}/@progress/data_flow_graph/vertices/slave/job_counter/completed/total".format(op.id)) == 2

    def test_files(self):
        create("file", "//tmp/a")
        write_file("//tmp/a", "data_a")
        create("file", "//tmp/b")
        write_file("//tmp/b", "data_b")

        op = vanilla(
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

    def test_stderr_table(self):
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

        stderrs = read_table("//tmp/stderr")
        per_task = Counter(row["data"] for row in stderrs)
        assert dict(per_task) == {"task_a\n": 3, "task_b\n": 2}

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

    def test_fail_on_revival(self):
        with pytest.raises(YtError):
            op = vanilla(
                dont_track=True,
                spec={
                    "tasks": {
                        "task_a": {
                            "job_count": 1,
                            "command": 'sleep 5',
                        },
                        "task_b": {
                            "job_count": 1,
                            "command": 'sleep 5',
                        },
                    },
                    "fail_on_job_restart": True,
                })
            self.Env.kill_schedulers()
            self.Env.start_schedulers()
            op.track()

    def test_abandon_job(self):
        # Abandoning vanilla job is ok.
        op = vanilla(
            dont_track=True,
            wait_for_jobs=True,
            spec={
                "tasks": {
                    "tasks_a": {
                        "job_count": 1,
                        "command": "exit 0",
                    }
                },
                "fail_on_job_restart": True
            })
        jobs = ls("//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op.id))
        assert len(jobs) == 1
        job_id = jobs[0]
        abandon_job(job_id)
        op.resume_jobs()
        op.track()

    def test_non_interruptible(self):
        op = vanilla(
            dont_track=True,
            wait_for_jobs=True,
            spec={
                "tasks": {
                    "tasks_a": {
                        "job_count": 1,
                        "command": "exit 0",
                    }
                },
                "fail_on_job_restart": True
            })
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
                            "command": " ; ".join([self.events.notify_event_cmd("job_started_a"), self.events.wait_event_cmd("finish_a")]),
                        },
                        "task_b": {
                            "job_count": 1,
                            "command": " ; ".join([self.events.notify_event_cmd("job_started_b"), self.events.wait_event_cmd("finish_b")]),
                        },
                    },
                    "fail_on_job_restart": True,
                })
            self.events.wait_event("job_started_a")
            self.events.wait_event("job_started_b")
            jobs = ls("//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op.id))
            assert len(jobs) == 2
            job_id = jobs[0]
            action(job_id)
            self.events.notify_event("finish_a")
            self.events.notify_event("finish_b")
            op.track()

##################################################################

class TestSchedulerVanillaCommandsMulticell(TestSchedulerVanillaCommands):
    NUM_SECONDARY_MASTER_CELLS = 2
