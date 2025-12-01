from yt_env_setup import YTEnvSetup

from yt_commands import authors, wait, events_on_fs, create, get, vanilla, create_tmpdir, read_table

from yt.common import YtError

import pytest

import datetime
import time

##################################################################


class SidecarVanillaBase(YTEnvSetup):
    ENABLE_MULTIDAEMON = False
    NUM_TEST_PARTITIONS = 3

    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    container_user_group_name = None

    USE_PORTO = None

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "job_environment": {
                    "container_user_group_name": container_user_group_name,
                },
            },
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 200,
        }
    }

    def get_docker_image(self):
        return None

    def start_operation(self, master_command, sidecar_command, sidecar_restart_policy="fail_on_error"):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "master": {
                        "job_count": 1,
                        "command": master_command,
                        "docker_image": self.get_docker_image(),
                        "sidecars": {
                            "sidecar1": {
                                "command": sidecar_command,
                                "docker_image": self.get_docker_image(),
                                "restart_policy": sidecar_restart_policy,
                            }
                        }
                    },
                },
            },
        )
        wait(lambda: len(get(op.get_path() + "/@progress/tasks")) == 1, ignore_exceptions=True)
        return op

    def start_operation_with_file(self, master_command, sidecar_command, sidecar_restart_policy="fail_on_error"):
        """
        Same as start_operation, but this will put sidecar_command into a file, making it possible to write
        a full bash script as the command.
        """

        # Prepare a sidecar bash file as it seems to be impossible to just concatenate several commands
        # under "command" section of sidecar definition, so we invoke a bash file.
        sidecar_cmds_file = create_tmpdir("sidecar_tmp") + "/sidecar_cmds"
        with open(sidecar_cmds_file, "w") as sidecar_cmds_file_open:
            sidecar_cmds_file_open.write(sidecar_command)

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "master": {
                        "job_count": 1,
                        "command": master_command,
                        "docker_image": self.get_docker_image(),
                        "files": [
                            sidecar_cmds_file,
                        ],
                        "sidecars": {
                            "sidecar1": {
                                "command": sidecar_command,
                                "docker_image": self.get_docker_image(),
                                "restart_policy": sidecar_restart_policy,
                            }
                        }
                    },
                },
            },
        )
        wait(lambda: len(get(op.get_path() + "/@progress/tasks")) == 1, ignore_exceptions=True)
        return op

    def ensure_operation_finish(self, op):
        op.track()

        data_flow_graph_path = op.get_path() + "/@progress/data_flow_graph"
        get(data_flow_graph_path)
        assert get(data_flow_graph_path + "/vertices/master/job_type") == "vanilla"
        assert get(data_flow_graph_path + "/vertices/master/job_counter/completed/total") == 1

        tasks = {}
        for task in get(op.get_path() + "/@progress/tasks"):
            tasks[task["task_name"]] = task

        assert tasks["master"]["job_type"] == "vanilla"
        assert tasks["master"]["job_counter"]["completed"]["total"] == 1

        assert get(op.get_path() + "/@progress/total_job_counter/completed/total") == 1
        assert op.get_job_count("aborted", from_orchid=False) == 0
        assert op.get_job_count("failed", from_orchid=False) == 0

    @authors("pavel-bash")
    def test_general(self):
        """
        Check that sidecars work in general by creating one alongside the master job and putting
        the corresponding event when it's launched.
        """
        master_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("master_job_started"),
                events_on_fs().wait_event_cmd("finish"),
            ]
        )
        sidecar_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("sidecar_job_started"),
                events_on_fs().wait_event_cmd("finish"),
            ]
        )

        op = self.start_operation(master_command, sidecar_command)

        # Wait until both master job and sidecar job are started, then finish the test.
        events_on_fs().wait_event("master_job_started", timeout=datetime.timedelta(1000))
        events_on_fs().wait_event("sidecar_job_started", timeout=datetime.timedelta(1000))
        events_on_fs().notify_event("finish")

        self.ensure_operation_finish(op)

    @authors("pavel-bash")
    def test_fail_no_command(self):
        """
        Command parameter for the sidecars is mandatory, so the operation must fail if it's empty.
        """
        master_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("master_job_started"),
                events_on_fs().wait_event_cmd("finish"),
            ]
        )

        with pytest.raises(YtError):
            vanilla(
                track=False,
                spec={
                    "tasks": {
                        "master": {
                            "job_count": 1,
                            "command": master_command,
                            "docker_image": self.get_docker_image(),
                            "sidecars": {
                                "sidecar1": {
                                    "docker_image": self.get_docker_image(),
                                }
                            }
                        },
                    },
                },
            )

    @authors("pavel-bash")
    def test_multiple_sidecars(self):
        """
        Check that multiple sidecars can be started with the main job.
        """
        master_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("master_job_started"),
                events_on_fs().wait_event_cmd("finish"),
            ]
        )
        sidecar1_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("sidecar1_job_started"),
                events_on_fs().wait_event_cmd("finish"),
            ]
        )
        sidecar2_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("sidecar2_job_started"),
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
                        "docker_image": self.get_docker_image(),
                        "sidecars": {
                            "sidecar1": {
                                "command": sidecar1_command,
                                "docker_image": self.get_docker_image(),
                            },
                            "sidecar2": {
                                "command": sidecar2_command,
                                "docker_image": self.get_docker_image(),
                            }
                        },
                    },
                },
            },
        )
        wait(lambda: len(get(op.get_path() + "/@progress/tasks")) == 1, ignore_exceptions=True)

        # Wait until master job and both sidecars are started, then finish the test.
        events_on_fs().wait_event("master_job_started", timeout=datetime.timedelta(1000))
        events_on_fs().wait_event("sidecar1_job_started", timeout=datetime.timedelta(1000))
        events_on_fs().wait_event("sidecar2_job_started", timeout=datetime.timedelta(1000))
        events_on_fs().notify_event("finish")

        self.ensure_operation_finish(op)

    @authors("pavel-bash")
    def test_restart_always_after_success(self):
        """
        Check the {restart_policy=always & sidecar-finished-with-success} case; we expect that
        the sidecar will be launched for the 2nd time, emitting the corresponding event.
        """
        master_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("master_job_started"),
                events_on_fs().wait_event_cmd("finish"),
            ]
        )
        sidecar_command = f"""
#!/bin/bash
if [ -f {events_on_fs()._get_event_filename("sidecar_first_job_started")} ]; then
  # It's the 2nd time the sidecar is launched.
  {events_on_fs().notify_event_cmd("sidecar_second_job_started")}
  {events_on_fs().wait_event_cmd("finish_sidecar_second")}
else
  {events_on_fs().notify_event_cmd("sidecar_first_job_started")}
  {events_on_fs().wait_event_cmd("finish_sidecar_first")}
fi
"""

        op = self.start_operation_with_file(master_command, sidecar_command, "always")

        # Wait until master job is started.
        events_on_fs().wait_event("master_job_started", timeout=datetime.timedelta(1000))

        # When the sidecar starts for the 1st time, finish its execution by sending the corresponding event.
        events_on_fs().wait_event("sidecar_first_job_started", timeout=datetime.timedelta(1000))
        events_on_fs().notify_event("finish_sidecar_first")

        # When the sidecar starts for 2nd time, it will emit another event; finish the test after that.
        events_on_fs().wait_event("sidecar_second_job_started", timeout=datetime.timedelta(1000))
        events_on_fs().notify_event("finish_sidecar_second")

        events_on_fs().notify_event("finish")

        self.ensure_operation_finish(op)

    @authors("pavel-bash")
    def test_restart_always_after_failure(self):
        """
        Check the {restart_policy=always & sidecar-finished-with-failure} case; we expect that
        the sidecar will be launched for the 2nd time, emitting the corresponding event.
        """
        master_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("master_job_started"),
                events_on_fs().wait_event_cmd("finish"),
            ]
        )
        sidecar_command = f"""
#!/bin/bash
if [ -f {events_on_fs()._get_event_filename("sidecar_first_job_started")} ]; then
  # It's the 2nd time the sidecar is launched.
  {events_on_fs().notify_event_cmd("sidecar_second_job_started")}
  {events_on_fs().wait_event_cmd("finish_sidecar_second")}
else
  {events_on_fs().notify_event_cmd("sidecar_first_job_started")}
  {events_on_fs().wait_event_cmd("finish_sidecar_first")}
  exit 1
fi
"""

        op = self.start_operation_with_file(master_command, sidecar_command, "always")

        # Wait until master job is started.
        events_on_fs().wait_event("master_job_started", timeout=datetime.timedelta(1000))

        # When the sidecar starts for the 1st time, it will finish the execution with error; we
        # expect it to restart.
        events_on_fs().wait_event("sidecar_first_job_started", timeout=datetime.timedelta(1000))
        events_on_fs().notify_event("finish_sidecar_first")

        # Check that the sidecar has started for the 2nd time and finish the test.
        events_on_fs().wait_event("sidecar_second_job_started", timeout=datetime.timedelta(1000))
        events_on_fs().notify_event("finish_sidecar_second")

        events_on_fs().notify_event("finish")

        self.ensure_operation_finish(op)

    @authors("pavel-bash")
    def test_restart_on_failure_after_success(self):
        """
        Check the {restart_policy=on_failure & sidecar-finished-with-success} case; the sidecar
        must not be restarted.
        """
        master_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("master_job_started"),
                events_on_fs().wait_event_cmd("finish"),
            ]
        )
        sidecar_command = f"""
#!/bin/bash
if [ -f {events_on_fs()._get_event_filename("sidecar_first_job_started")} ]; then
  {events_on_fs().notify_event_cmd("sidecar_second_job_started")}
else
  {events_on_fs().notify_event_cmd("sidecar_first_job_started")}
  {events_on_fs().wait_event_cmd("finish_sidecar_first")}
fi
"""

        op = self.start_operation_with_file(master_command, sidecar_command, "on_failure")

        # Wait until master job is started.
        events_on_fs().wait_event("master_job_started", timeout=datetime.timedelta(1000))

        # Wait until the sidecar is started.
        events_on_fs().wait_event("sidecar_first_job_started", timeout=datetime.timedelta(1000))
        events_on_fs().notify_event("finish_sidecar_first")

        # Wait here a little bit; if the code is faulty, the sidecar is going to start for the 2nd time
        # during this wait.
        time.sleep(2)

        events_on_fs().notify_event("finish")

        self.ensure_operation_finish(op)

        # Check that the sidecar hasn't started for the 2nd time.
        assert not events_on_fs().check_event("sidecar_second_job_started")

    @authors("pavel-bash")
    def test_restart_on_failure_after_failure(self):
        """
        Check the {restart_policy=on_failure & sidecar-finished-with-failure} case; we expect that
        the sidecar will be launched for the 2nd time, emitting the corresponding event.
        """
        master_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("master_job_started"),
                events_on_fs().wait_event_cmd("finish"),
            ]
        )
        sidecar_command = f"""
#!/bin/bash
if [ -f {events_on_fs()._get_event_filename("sidecar_first_job_started")} ]; then
  # It's the 2nd time the sidecar is launched.
  {events_on_fs().notify_event_cmd("sidecar_second_job_started")}
  {events_on_fs().wait_event_cmd("finish_sidecar_second")}
else
  {events_on_fs().notify_event_cmd("sidecar_first_job_started")}
  {events_on_fs().wait_event_cmd("finish_sidecar_first")}
  exit 1
fi
"""

        op = self.start_operation_with_file(master_command, sidecar_command, "on_failure")

        # Wait until master job is started.
        events_on_fs().wait_event("master_job_started", timeout=datetime.timedelta(1000))

        # When the sidecar starts for the 1st time, it will finish the execution with error; we
        # expect it to restart.
        events_on_fs().wait_event("sidecar_first_job_started", timeout=datetime.timedelta(1000))
        events_on_fs().notify_event("finish_sidecar_first")

        # Check that the sidecar has started for the 2nd time and finish the test.
        events_on_fs().wait_event("sidecar_second_job_started", timeout=datetime.timedelta(1000))
        events_on_fs().notify_event("finish_sidecar_second")

        events_on_fs().notify_event("finish")

        self.ensure_operation_finish(op)

    @authors("pavel-bash")
    def test_fail_on_error_after_success(self):
        """
        Check the {restart_policy=fail_on_error & sidecar-finished-with-success} case; the sidecar
        must not be restarted.
        """
        master_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("master_job_started"),
                events_on_fs().wait_event_cmd("finish"),
            ]
        )
        sidecar_command = f"""
#!/bin/bash
if [ -f {events_on_fs()._get_event_filename("sidecar_first_job_started")} ]; then
  {events_on_fs().notify_event_cmd("sidecar_second_job_started")}
else
  {events_on_fs().notify_event_cmd("sidecar_first_job_started")}
  {events_on_fs().wait_event_cmd("finish_sidecar_first")}
fi
"""

        op = self.start_operation_with_file(master_command, sidecar_command, "fail_on_error")

        # Wait until master job is started.
        events_on_fs().wait_event("master_job_started", timeout=datetime.timedelta(1000))

        # Wait until the sidecar is started.
        events_on_fs().wait_event("sidecar_first_job_started", timeout=datetime.timedelta(1000))
        events_on_fs().notify_event("finish_sidecar_first")

        # Wait here a little bit; if the code is faulty, the sidecar is going to start for the 2nd time
        # during this wait.
        time.sleep(2)

        events_on_fs().notify_event("finish")

        self.ensure_operation_finish(op)

        # Check that the sidecar hasn't started for the 2nd time.
        assert not events_on_fs().check_event("sidecar_second_job_started")

    @authors("pavel-bash")
    def test_fail_on_error_after_failure(self):
        """
        Check the {restart_policy=fail_on_error & sidecar-finished-with-failure} case; we expect that
        the whole job will be failed because of this.
        """
        master_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("master_job_started"),
                events_on_fs().wait_event_cmd("finish"),  # Will not arrive.
            ]
        )
        sidecar_command = f"""
#!/bin/bash
if [ -f {events_on_fs()._get_event_filename("sidecar_first_job_started")} ]; then
  {events_on_fs().notify_event_cmd("sidecar_second_job_started")}
else
  {events_on_fs().notify_event_cmd("sidecar_first_job_started")}
  {events_on_fs().wait_event_cmd("finish_sidecar_first")}
  exit 1
fi
"""

        op = self.start_operation_with_file(master_command, sidecar_command, "fail_on_error")

        # Wait until master job is started.
        events_on_fs().wait_event("master_job_started", timeout=datetime.timedelta(1000))

        # The sidecar's execution will finish with error.
        events_on_fs().wait_event("sidecar_first_job_started", timeout=datetime.timedelta(1000))
        events_on_fs().notify_event("finish_sidecar_first")

        # Check that the whole operation has failed, and the sidecar hasn't restarted.
        op.wait_for_state("failed")
        assert not events_on_fs().check_event("sidecar_second_job_started")

    @authors("pavel-bash")
    def test_mounts(self):
        if self.USE_PORTO:
            pytest.skip("Not implemented for porto")

        """
        Check that the mounts are indeed shared between the main job and all sidecars; we create a file
        and write some data into it in a sidecar job, then the master job reads the file and outputs
        its contents into the output table.
        """
        output_table = "//tmp/output_table"
        create("table", output_table)

        shared_file_name = "shared_file"

        master_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("master_job_started"),
                events_on_fs().wait_event_cmd("master_proceed"),
                f"cat {shared_file_name}",
                events_on_fs().notify_event_cmd("master_job_cat"),
                events_on_fs().wait_event_cmd("finish"),
            ]
        )
        sidecar_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("sidecar_job_started"),
                f"echo '{{secret=sidecar}}' >> {shared_file_name}",
                events_on_fs().notify_event_cmd("sidecar_job_wrote"),
                events_on_fs().wait_event_cmd("finish"),
            ]
        )

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "master": {
                        "job_count": 1,
                        "output_table_paths": [output_table],
                        "command": master_command,
                        "docker_image": self.get_docker_image(),
                        "sidecars": {
                            "sidecar1": {
                                "command": sidecar_command,
                                "docker_image": self.get_docker_image(),
                                "restart_policy": "fail_on_error",
                            }
                        }
                    },
                },
            },
        )
        wait(lambda: len(get(op.get_path() + "/@progress/tasks")) == 1, ignore_exceptions=True)

        # Wait until both master job and sidecar are started.
        events_on_fs().wait_event("master_job_started", timeout=datetime.timedelta(1000))
        events_on_fs().wait_event("sidecar_job_started", timeout=datetime.timedelta(1000))

        # Wait until sidecar job has written something into the file.
        events_on_fs().wait_event("sidecar_job_wrote", timeout=datetime.timedelta(1000))

        # Allow the master to proceed and wait until it finishes with the task.
        events_on_fs().notify_event("master_proceed")
        events_on_fs().wait_event("master_job_cat", timeout=datetime.timedelta(1000))

        events_on_fs().notify_event("finish")

        self.ensure_operation_finish(op)

        # Check that we can see the sidecar's file contents in the output table.
        output_table_contents = read_table(output_table)
        print(output_table_contents)
        assert read_table(output_table) == [{"secret": "sidecar"}]

    @authors("krasovav")
    def test_signal_shutdown(self):
        if not self.USE_PORTO:
            pytest.skip("Not implemented for cri")

        master_command = " ; ".join(
            [
                events_on_fs().notify_event_cmd("master_job_started"),
                events_on_fs().wait_event_cmd("finish"),
            ]
        )
        sidecar_command = " ; ".join(
            [
                "trap '" + events_on_fs().notify_event_cmd("signal_traped"),
                "sleep 10",
                events_on_fs().notify_event_cmd("signal_processed"),
                "' SIGUSR1",
                events_on_fs().notify_event_cmd("sidecar_job_started"),
                events_on_fs().wait_event_cmd("unreachable_event"),
            ]
        )

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "master": {
                        "job_count": 1,
                        "command": master_command,
                        "docker_image": self.get_docker_image(),
                        "sidecars": {
                            "sidecar": {
                                "command": sidecar_command,
                                "docker_image": self.get_docker_image(),
                                "graceful_shutdown": {
                                    "signal": "SIGUSR1",
                                    "timeout": 1000
                                },
                            },
                        },
                    },
                },
            },
        )

        wait(lambda: len(get(op.get_path() + "/@progress/tasks")) == 1, ignore_exceptions=True)
        events_on_fs().wait_event("master_job_started", timeout=datetime.timedelta(1000))
        events_on_fs().wait_event("sidecar_job_started", timeout=datetime.timedelta(1000))

        events_on_fs().notify_event("finish")

        events_on_fs().wait_event("signal_traped", timeout=datetime.timedelta(1000))
        wait(lambda: op.get_job_count("completed") == 1)
        assert not events_on_fs().check_event("signal_processed")


##################################################################


class TestPortoSidecar(SidecarVanillaBase):
    USE_PORTO = True


##################################################################


class TestCriSidecar(SidecarVanillaBase):
    JOB_ENVIRONMENT_TYPE = "cri"

    def __init__(self):
        self.container_user_group_name = "docker"

    def get_docker_image(self):
        return self.Env.yt_config.default_docker_image
