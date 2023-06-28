from yt_env_setup import YTEnvSetup, Restarter, CONTROLLER_AGENTS_SERVICE

from yt_commands import (
    authors, wait, events_on_fs, create, ls, get, set, exists,
    create_domestic_medium, create_account, read_table,
    write_table, map,
    start_transaction, abort_transaction,
    create_account_resource_usage_lease, update_controller_agent_config,
    abort_job, run_test_vanilla, extract_statistic_v2 as extract_statistic,
    with_breakpoint, wait_breakpoint, wait_no_assert)

from yt.common import YtError, update

import pytest

import os
import time
import shutil

##################################################################


class TestDiskUsagePorto(YTEnvSetup):
    USE_PORTO = True

    NUM_SCHEDULERS = 1
    NUM_MASTERS = 1
    NUM_NODES = 1
    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "locations": [{"disk_quota": 1024 * 1024, "disk_usage_watermark": 0}],
                "disk_resources_update_period": 100,
            },
            "job_controller": {
                "waiting_jobs_timeout": 1000,
                "resource_limits": {"user_slots": 3, "cpu": 3.0},
            },
            "min_required_disk_space": 0,
        },
        "data_node": {
            "volume_manager": {
                "enable_disk_quota": False
            }
        }
    }

    DELTA_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "safe_scheduler_online_time": 500,
        }
    }

    @classmethod
    def modify_node_config(cls, config):
        os.makedirs(cls.fake_default_disk_path)
        config["exec_node"]["slot_manager"]["locations"][0]["path"] = cls.fake_default_disk_path

    def _init_tables(self):
        tables = ["//tmp/t1", "//tmp/t2", "//tmp/t3"]
        for table in tables:
            create("table", table)
        write_table(tables[0], [{"foo": "bar"} for _ in range(10)])
        return tables

    def run_test(self, tables, fatty_options):
        options = {
            "in_": tables[0],
            "out": tables[1],
            "track": False,
        }

        options.update(fatty_options)

        first = map(**options)

        events_on_fs().wait_event("file_written")

        check_op = {
            "in_": tables[0],
            "out": tables[2],
            "command": "true",
            "spec": {
                "mapper": {"disk_space_limit": 1024 * 1024 // 2},
                "max_failed_job_count": 1,
            },
        }

        op = map(track=False, **check_op)
        wait(lambda: exists(op.get_path() + "/controller_orchid/progress/jobs"))
        for type in ("running", "aborted", "failed"):
            assert op.get_job_count(type) == 0
        op.abort()

        events_on_fs().notify_event("finish_job")
        first.track()

        map(**check_op)

    @authors("astiunov")
    def test_lack_space_node(self):
        tables = self._init_tables()
        options = {
            "command": " ; ".join(
                [
                    "dd if=/dev/zero of=zeros.txt count=1500",
                    events_on_fs().notify_event_cmd("file_written"),
                    events_on_fs().wait_event_cmd("finish_job"),
                ]
            )
        }

        self.run_test(tables, options)

    @authors("astiunov")
    def test_lack_space_node_with_quota(self):
        tables = self._init_tables()
        options = {
            "command": " ; ".join(
                [
                    "true",
                    events_on_fs().notify_event_cmd("file_written"),
                    events_on_fs().wait_event_cmd("finish_job"),
                ]
            ),
            "spec": {
                "mapper": {"disk_space_limit": 1024 * 1024 * 2 // 3},
                "max_failed_job_count": 1,
            },
        }

        self.run_test(tables, options)

    @authors("ignat")
    def test_not_available_nodes(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}])

        op = map(
            track=False,
            command="cat",
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "mapper": {"disk_space_limit": 2 * 1024 * 1024},
                "max_failed_job_count": 1,
            },
        )
        op.ensure_running()

        wait(lambda: op.get_state() == "failed")

    @authors("ignat")
    def test_scheduled_after_wait(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")
        write_table("//tmp/t1", [{"foo": "bar"}])

        op1 = map(
            track=False,
            command=with_breakpoint("BREAKPOINT"),
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "mapper": {"disk_space_limit": 2 * 1024 * 1024 // 3},
                "max_failed_job_count": 1,
            },
        )
        wait_breakpoint()

        assert op1.get_job_count("running") == 1

        op2 = map(
            track=False,
            command="sleep 1000",
            in_="//tmp/t1",
            out="//tmp/t3",
            spec={
                "mapper": {"disk_space_limit": 2 * 1024 * 1024 // 3},
                "max_failed_job_count": 1,
            },
        )
        op2.ensure_running()

        time.sleep(2)

        for type in ("running", "aborted", "failed"):
            assert op2.get_job_count(type) == 0

        op1.abort()

        wait(lambda: op2.get_job_count("running") == 1)
        op2.abort()

    @authors("ignat")
    def test_statistics(self):
        op = run_test_vanilla(
            command=" ; ".join([
                "dd if=/dev/zero of=zeros.txt count=500 bs=1024",
                events_on_fs().breakpoint_cmd("file_written"),
                "rm zeros.txt",
                events_on_fs().breakpoint_cmd("file_removed"),
            ]),
            task_patch={"disk_space_limit": 1024 * 1024},
            spec={"max_failed_job_count": 1},
        )

        events_on_fs().wait_breakpoint("file_written")

        def check_disk_statistics(usage, max_usage, limit):
            statistics = op.get_statistics()
            assert extract_statistic(
                statistics,
                key="user_job.disk.usage",
                job_state="running",
                job_type=None,
                summary_type="max") == usage

            assert extract_statistic(
                statistics,
                key="user_job.disk.max_usage",
                job_state="running",
                job_type=None,
                summary_type="max") == max_usage

            assert extract_statistic(
                statistics,
                key="user_job.disk.limit",
                job_state="running",
                job_type=None,
                summary_type="max") == limit

        wait_no_assert(lambda: check_disk_statistics(usage=500 * 1024, max_usage=500 * 1024, limit=1024 * 1024))

        events_on_fs().release_breakpoint("file_written")
        events_on_fs().wait_breakpoint("file_removed")

        wait_no_assert(lambda: check_disk_statistics(usage=0, max_usage=500 * 1024, limit=1024 * 1024))


##################################################################

class DiskMediumTestConfiguration(object):
    MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1
        }
    }

    CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "safe_scheduler_online_time": 60000,
        },
        "cluster_connection": {
            "medium_directory_synchronizer": {
                "sync_period": 100,
            }
        },
    }

    SCHEDULER_CONFIG = {
        "cluster_connection": {
            "medium_directory_synchronizer": {
                "sync_period": 100,
            }
        },
    }


class TestDiskMediumsPorto(YTEnvSetup, DiskMediumTestConfiguration):
    NUM_SCHEDULERS = 1
    NUM_MASTERS = 1
    NUM_NODES = 1

    DELTA_MASTER_CONFIG = DiskMediumTestConfiguration.MASTER_CONFIG
    DELTA_SCHEDULER_CONFIG = DiskMediumTestConfiguration.SCHEDULER_CONFIG
    DELTA_CONTROLLER_AGENT_CONFIG = DiskMediumTestConfiguration.CONTROLLER_AGENT_CONFIG
    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "disk_resources_update_period": 100,
            },
            "job_controller": {
                "waiting_jobs_timeout": 1000,
                "resource_limits": {"user_slots": 3, "cpu": 3.0},
            },
            "min_required_disk_space": 0,
        },
        "data_node": {
            "volume_manager": {
                "enable_disk_quota": False
            }
        }
    }

    USE_PORTO = True

    @classmethod
    def modify_node_config(cls, config):
        for disk in (cls.fake_default_disk_path, cls.fake_ssd_disk_path):
            if os.path.exists(disk):
                shutil.rmtree(disk)
            os.makedirs(disk)

        config["exec_node"]["slot_manager"]["locations"] = [
            {
                "path": cls.fake_default_disk_path,
                "disk_quota": 10 * 1024 * 1024,
                "disk_usage_watermark": 0,
            },
            {
                "path": cls.fake_ssd_disk_path,
                "disk_quota": 2 * 1024 * 1024,
                "disk_usage_watermark": 0,
                "medium_name": "ssd",
            },
        ]

    @classmethod
    def on_masters_started(cls):
        create_domestic_medium("ssd")

    @authors("ignat")
    def test_ssd_request(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        create("table", "//tmp/out")

        op = map(
            command="cat; echo $(pwd) >&2",
            in_="//tmp/in",
            out="//tmp/out",
            spec={
                "mapper": {
                    "disk_request": {
                        "disk_space": 2 * 1024 * 1024,
                        "medium_name": "ssd",
                    },
                },
                "max_failed_job_count": 1,
            },
        )

        assert read_table("//tmp/out") == [{"foo": "bar"}]

        jobs = op.list_jobs()
        assert len(jobs) == 1
        assert op.read_stderr(jobs[0]).startswith(self.fake_ssd_disk_path.encode("ascii"))

    @authors("ignat")
    def test_unfeasible_ssd_request(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        create("table", "//tmp/out")

        op = map(
            command="cat; echo $(pwd) >&2",
            in_="//tmp/in",
            out="//tmp/out",
            spec={
                "mapper": {
                    "disk_request": {
                        "disk_space": 3 * 1024 * 1024,
                        "medium_name": "ssd",
                    },
                },
                "max_failed_job_count": 1,
            },
            track=False,
        )

        wait(lambda: exists(op.get_path() + "/controller_orchid/progress/jobs"))
        for type in ("running", "aborted", "failed"):
            assert op.get_job_count(type) == 0
        op.abort()

    @authors("ignat")
    def test_unknown_medium(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        create("table", "//tmp/out")

        with pytest.raises(YtError):
            map(
                command="cat; echo $(pwd) >&2",
                in_="//tmp/in",
                out="//tmp/out",
                spec={
                    "mapper": {
                        "disk_request": {
                            "disk_space": 1024 * 1024,
                            "medium_name": "unknown",
                        },
                    },
                    "max_failed_job_count": 1,
                },
            )

    @authors("ignat")
    def test_multiple_feasible_disk_requests(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1

        node = nodes[0]
        node_path = "//sys/cluster_nodes/{}".format(node)
        set("{}/@resource_limits_overrides/cpu".format(node_path), 0)
        wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/{}/resource_limits/cpu".format(node)) == 0.0)

        def start_op(index):
            output_table = "//tmp/out" + str(index)
            create("table", output_table)
            return map(
                command="cat; echo $(pwd) >&2",
                in_="//tmp/in",
                out=output_table,
                spec={
                    "mapper": {
                        "disk_request": {
                            "disk_space": 1 * 1024 * 1024,
                            "medium_name": "ssd",
                        },
                    },
                    "max_failed_job_count": 1,
                },
                track=False,
            )

        op1 = start_op(1)
        op2 = start_op(2)

        for op in (op1, op2):
            wait(lambda: exists(op.get_path() + "/controller_orchid/progress/jobs"))
            for type in ("running", "aborted", "failed"):
                assert op.get_job_count(type) == 0

        set("{}/@resource_limits_overrides/cpu".format(node_path), 3.0)

        op1.track()
        op2.track()

    @authors("ignat")
    def test_multiple_unfeasible_disk_requests(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1

        node = nodes[0]
        set("//sys/cluster_nodes/{0}/@resource_limits_overrides/cpu".format(node), 0)

        def start_op(index, medium_type, disk_space_gb):
            output_table = "//tmp/out" + str(index)
            create("table", output_table)
            return map(
                command="cat; echo $(pwd) >&2",
                in_="//tmp/in",
                out=output_table,
                spec={
                    "mapper": {
                        "disk_request": {
                            "disk_space": disk_space_gb * 1024 * 1024,
                            "medium_name": medium_type,
                        },
                    },
                    "max_failed_job_count": 1,
                    "sanity_check_delay": 60000,
                },
                track=False,
            )

        op1 = start_op(1, "ssd", 1)
        op2 = start_op(2, "ssd", 2)
        op3 = start_op(3, "ssd", 3)
        op4 = start_op(4, "default", 5)
        op5 = start_op(5, "default", 5)
        op6 = start_op(6, "default", 6)

        for op in (op1, op2, op3, op4, op5, op6):
            wait(lambda: exists(op.get_path() + "/controller_orchid/progress/jobs"))
            for type in ("running", "aborted", "failed"):
                assert op.get_job_count(type) == 0

        set("//sys/cluster_nodes/{0}/@resource_limits_overrides/cpu".format(node), 3.0)

        op1.track()
        op2.track()
        op4.track()
        op5.track()
        op6.track()

        assert op3.get_state() == "running"
        for type in ("running", "aborted", "failed"):
            assert op3.get_job_count(type) == 0


class TestDiskMediumRenamePorto(YTEnvSetup, DiskMediumTestConfiguration):
    NUM_SCHEDULERS = 1
    NUM_MASTERS = 1
    NUM_NODES = 1

    DELTA_MASTER_CONFIG = DiskMediumTestConfiguration.MASTER_CONFIG
    DELTA_SCHEDULER_CONFIG = DiskMediumTestConfiguration.SCHEDULER_CONFIG
    DELTA_CONTROLLER_AGENT_CONFIG = DiskMediumTestConfiguration.CONTROLLER_AGENT_CONFIG
    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "disk_resources_update_period": 100,
            },
            "job_controller": {
                "waiting_jobs_timeout": 1000,
                "resource_limits": {"user_slots": 3, "cpu": 3.0},
            },
            "min_required_disk_space": 0,
        },
        "data_node": {
            "volume_manager": {
                "enable_disk_quota": False
            }
        }
    }

    USE_PORTO = True

    @classmethod
    def modify_node_config(cls, config):
        for disk in (cls.fake_default_disk_path, cls.fake_ssd_disk_path):
            if os.path.exists(disk):
                shutil.rmtree(disk)
            os.makedirs(disk)

        config["exec_node"]["slot_manager"]["locations"] = [
            {
                "path": cls.fake_default_disk_path,
                "disk_quota": 10 * 1024 * 1024,
                "disk_usage_watermark": 0,
            },
            {
                "path": cls.fake_ssd_disk_path,
                "disk_quota": 2 * 1024 * 1024,
                "disk_usage_watermark": 0,
                "medium_name": "ssd",
            },
        ]

    @classmethod
    def on_masters_started(cls):
        create_domestic_medium("ssd")

    @authors("ignat")
    def test_media_rename(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1

        node = nodes[0]
        set("//sys/cluster_nodes/{0}/@resource_limits_overrides/cpu".format(node), 0)

        def start_op(index, medium_type, track):
            output_table = "//tmp/out" + str(index)
            create("table", output_table)
            return map(
                command="cat; echo $(pwd) >&2",
                in_="//tmp/in",
                out=output_table,
                spec={
                    "mapper": {
                        "disk_request": {
                            "disk_space": 1024 * 1024,
                            "medium_name": medium_type,
                        },
                    },
                    "max_failed_job_count": 1,
                },
                track=track,
            )

        op = start_op(1, "ssd", track=False)
        op.ensure_running()

        set("//sys/media/ssd/@name", "ssd_renamed")

        wait(lambda: "ssd_renamed" in ls("//sys/scheduler/orchid/scheduler/cluster/medium_directory"))

        controller_agents = ls("//sys/controller_agents/instances")
        assert len(controller_agents) == 1
        controller_agent = controller_agents[0]
        wait(
            lambda: "ssd_renamed"
            in ls(
                "//sys/controller_agents/instances/{}/orchid/controller_agent/medium_directory".format(controller_agent)
            )
        )

        with pytest.raises(YtError):
            start_op(2, "ssd", track=True)

        set("//sys/cluster_nodes/{0}/@resource_limits_overrides/cpu".format(node), 3.0)
        op.track()

        start_op(3, "ssd_renamed", track=True)


class TestDefaultDiskMediumPorto(YTEnvSetup, DiskMediumTestConfiguration):
    NUM_SCHEDULERS = 1
    NUM_MASTERS = 1
    NUM_NODES = 1

    DELTA_MASTER_CONFIG = DiskMediumTestConfiguration.MASTER_CONFIG
    DELTA_SCHEDULER_CONFIG = DiskMediumTestConfiguration.SCHEDULER_CONFIG
    DELTA_CONTROLLER_AGENT_CONFIG = DiskMediumTestConfiguration.CONTROLLER_AGENT_CONFIG
    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "disk_resources_update_period": 100,
            },
            "job_controller": {"resource_limits": {"user_slots": 1, "cpu": 1.0}},
            "min_required_disk_space": 0,
        },
        "data_node": {
            "volume_manager": {
                "enable_disk_quota": False
            }
        }
    }

    USE_PORTO = True

    @classmethod
    def modify_node_config(cls, config):
        for disk in (cls.fake_default_disk_path, cls.fake_ssd_disk_path):
            if os.path.exists(disk):
                shutil.rmtree(disk)
            os.makedirs(disk)

        config["exec_node"]["slot_manager"]["locations"] = [
            {
                "path": cls.fake_ssd_disk_path,
                "disk_quota": 2 * 1024 * 1024,
                "disk_usage_watermark": 0,
                "medium_name": "ssd",
            }
        ]
        config["exec_node"]["slot_manager"]["default_medium_name"] = "hdd"

    @classmethod
    def on_masters_started(cls):
        create_domestic_medium("hdd")
        create_domestic_medium("ssd")

    @authors("ignat")
    def test_default_medium_on_node(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        create("table", "//tmp/out")

        def start_op(medium_type, track):
            disk_request = {"disk_space": 1024 * 1024}
            if medium_type is not None:
                disk_request["medium_name"] = medium_type

            return map(
                command="cat; echo $(pwd) >&2",
                in_="//tmp/in",
                out="//tmp/out",
                spec={
                    "mapper": {
                        "disk_request": disk_request
                    },
                    "max_failed_job_count": 1,
                },
                track=track,
            )

        # Node has disk with SSD.
        start_op("ssd", track=True)

        # Node has no disks with SSD.
        op = start_op("hdd", track=False)
        op.wait_for_state("running")
        time.sleep(5)
        assert op.get_state() == "running"
        assert op.get_job_count("running") == 0
        assert op.get_job_count("aborted") == 0
        op.abort()

        # Default medium is also HDD.
        op = start_op(None, track=False)
        op.wait_for_state("running")
        time.sleep(5)
        assert op.get_state() == "running"
        assert op.get_job_count("running") == 0
        assert op.get_job_count("aborted") == 0
        op.abort()


class TestDefaultDiskMediumWithUnspecifiedMediumPorto(YTEnvSetup, DiskMediumTestConfiguration):
    NUM_SCHEDULERS = 1
    NUM_MASTERS = 1
    NUM_NODES = 1

    DELTA_MASTER_CONFIG = DiskMediumTestConfiguration.MASTER_CONFIG
    DELTA_SCHEDULER_CONFIG = DiskMediumTestConfiguration.SCHEDULER_CONFIG
    DELTA_CONTROLLER_AGENT_CONFIG = DiskMediumTestConfiguration.CONTROLLER_AGENT_CONFIG
    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "disk_resources_update_period": 100,
            },
            "job_controller": {"resource_limits": {"user_slots": 3, "cpu": 3.0}},
            "min_required_disk_space": 0,
        },
        "data_node": {
            "volume_manager": {
                "enable_disk_quota": False
            }
        }
    }

    USE_PORTO = True

    @classmethod
    def modify_node_config(cls, config):
        for disk in (cls.fake_default_disk_path, cls.fake_ssd_disk_path):
            if os.path.exists(disk):
                shutil.rmtree(disk)
            os.makedirs(disk)

        config["exec_node"]["slot_manager"]["locations"] = [
            {
                "path": cls.fake_ssd_disk_path,
                "disk_quota": 2 * 1024 * 1024,
                "disk_usage_watermark": 0,
                "medium_name": "ssd",
            }
        ]
        config["exec_node"]["slot_manager"]["default_medium_name"] = "ssd"

    @classmethod
    def on_masters_started(cls):
        create_domestic_medium("hdd")
        create_domestic_medium("ssd")

    @authors("ignat")
    def test_default_medium_on_node(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        create("table", "//tmp/out")

        def start_op(medium_type, track):
            disk_request = {"disk_space": 1024 * 1024}
            if medium_type is not None:
                disk_request["medium_name"] = medium_type

            return map(
                command="cat; echo $(pwd) >&2",
                in_="//tmp/in",
                out="//tmp/out",
                spec={
                    "mapper": {
                        "disk_request": disk_request
                    },
                    "max_failed_job_count": 1,
                },
                track=track,
            )

        # Default medium is also SSD.
        start_op(None, track=True)

        # Node has disk with SSD.
        start_op("ssd", track=True)

    @authors("ignat")
    def test_unspecified_disk_request(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        create("table", "//tmp/out")

        map(command="cat",
            in_="//tmp/in",
            out="//tmp/out",
            spec={"max_failed_job_count": 1})

    @authors("ignat")
    def test_multiple_operations(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        create("table", "//tmp/out3")

        def start_op(medium_type, index, track):
            disk_request = {"disk_space": 768 * 1024}
            if medium_type is not None:
                disk_request["medium_name"] = medium_type

            return map(
                command="sleep 1000; echo $(pwd) >&2",
                in_="//tmp/in",
                out="//tmp/out" + str(index),
                spec={
                    "mapper": {
                        "disk_request": disk_request
                    },
                    "max_failed_job_count": 1,
                },
                track=track,
            )

        op1 = start_op(None, 1, track=False)
        op2 = start_op(None, 2, track=False)
        op3 = start_op(None, 3, track=False)

        wait(lambda: sum([op.get_job_count("running") for op in (op1, op2, op3)]) == 2)

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node_orchid_jobs_path = "//sys/cluster_nodes/{}/orchid/job_controller/active_jobs/scheduler".format(nodes[0])

        wait(lambda: len(ls(node_orchid_jobs_path)) == 2)

        aborted1 = op1.get_job_count("aborted")
        aborted2 = op2.get_job_count("aborted")
        aborted3 = op3.get_job_count("aborted")

        time.sleep(2)

        # Check that no new aborted jobs after 2 seconds.
        assert aborted1 == op1.get_job_count("aborted")
        assert aborted2 == op2.get_job_count("aborted")
        assert aborted3 == op3.get_job_count("aborted")


class TestDefaultDiskMediumWithUnspecifiedMediumAndMultipleSlotsPorto(YTEnvSetup, DiskMediumTestConfiguration):
    NUM_SCHEDULERS = 1
    NUM_MASTERS = 1
    NUM_NODES = 1

    DELTA_MASTER_CONFIG = DiskMediumTestConfiguration.MASTER_CONFIG
    DELTA_SCHEDULER_CONFIG = DiskMediumTestConfiguration.SCHEDULER_CONFIG
    DELTA_CONTROLLER_AGENT_CONFIG = DiskMediumTestConfiguration.CONTROLLER_AGENT_CONFIG
    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "disk_resources_update_period": 100,
            },
            "job_controller": {"resource_limits": {"user_slots": 1, "cpu": 1.0}},
            "min_required_disk_space": 0,
        },
        "data_node": {
            "volume_manager": {
                "enable_disk_quota": False
            }
        }
    }

    USE_PORTO = True

    @classmethod
    def modify_node_config(cls, config):
        for disk in (cls.fake_default_disk_path, cls.fake_ssd_disk_path):
            if os.path.exists(disk):
                shutil.rmtree(disk)
            os.makedirs(disk)

        config["exec_node"]["slot_manager"]["locations"] = [
            {
                "path": cls.fake_default_disk_path,
                "disk_quota": 2 * 1024 * 1024,
                "disk_usage_watermark": 0,
                "medium_name": "hdd",
            },
            {
                "path": cls.fake_ssd_disk_path,
                "disk_quota": 2 * 1024 * 1024,
                "disk_usage_watermark": 0,
                "medium_name": "ssd",
            },
        ]
        config["exec_node"]["slot_manager"]["default_medium_name"] = "ssd"

    @classmethod
    def on_masters_started(cls):
        create_domestic_medium("hdd")
        create_domestic_medium("ssd")

    @authors("ignat")
    def test_slot_index(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        create("table", "//tmp/out")

        def start_op(medium_type, track):
            disk_request = {"disk_space": 1024 * 1024}
            if medium_type is not None:
                disk_request["medium_name"] = medium_type

            return map(
                command="cat; echo $(pwd) >&2; sleep 100",
                in_="//tmp/in",
                out="//tmp/out",
                spec={
                    "mapper": {
                        "disk_request": disk_request
                    },
                    "max_failed_job_count": 1,
                },
                track=track,
            )

        op = start_op(None, track=False)
        wait(lambda: op.get_running_jobs())

        nodes = ls("//sys/cluster_nodes")
        jobs = list(op.get_running_jobs().keys())
        assert len(nodes) == 1
        assert len(jobs) == 1

        node_orchid_job_path = "//sys/cluster_nodes/{}/orchid/job_controller/active_jobs/scheduler/{}"\
                               .format(nodes[0], jobs[0])
        wait(lambda: exists(node_orchid_job_path))
        wait(lambda: get(node_orchid_job_path + "/exec_attributes/medium_name") == "ssd")


class TestDiskMediumAccounting(YTEnvSetup, DiskMediumTestConfiguration):
    NUM_SCHEDULERS = 1
    NUM_MASTERS = 1
    NUM_NODES = 1

    DELTA_MASTER_CONFIG = DiskMediumTestConfiguration.MASTER_CONFIG
    DELTA_SCHEDULER_CONFIG = DiskMediumTestConfiguration.SCHEDULER_CONFIG
    DELTA_CONTROLLER_AGENT_CONFIG = update(
        DiskMediumTestConfiguration.CONTROLLER_AGENT_CONFIG,
        {
            "controller_agent": {
                "obligatory_account_mediums": ["ssd"],
                "snapshot_period": 500,
                "snapshot_writer": {
                    "upload_replication_factor": 1,
                    "min_upload_replication_factor": 1,
                }
            },
        })
    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "disk_resources_update_period": 100,
            },
            "job_controller": {"resource_limits": {"user_slots": 2, "cpu": 2.0}},
            "min_required_disk_space": 0,
        },
        "data_node": {
            "volume_manager": {
                "enable_disk_quota": False
            }
        }
    }

    USE_PORTO = True

    @classmethod
    def modify_node_config(cls, config):
        for disk in (cls.fake_default_disk_path, cls.fake_ssd_disk_path):
            if os.path.exists(disk):
                shutil.rmtree(disk)
            os.makedirs(disk)

        config["exec_node"]["slot_manager"]["locations"] = [
            {
                "path": cls.fake_default_disk_path,
                "disk_quota": 2 * 1024 * 1024,
                "disk_usage_watermark": 0,
                "medium_name": "hdd",
            },
            {
                "path": cls.fake_ssd_disk_path,
                "disk_quota": 2 * 1024 * 1024,
                "disk_usage_watermark": 0,
                "medium_name": "ssd",
            },
        ]
        config["exec_node"]["slot_manager"]["default_medium_name"] = "ssd"

    @classmethod
    def on_masters_started(cls):
        create_domestic_medium("hdd")
        create_domestic_medium("ssd")

    @authors("ignat")
    def test_accounting(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        create("table", "//tmp/out")

        create_account("my_account")
        set("//sys/accounts/my_account/@resource_limits/disk_space_per_medium/ssd", 1024 * 1024)

        def start_op(medium_type, account, disk_space, track, sleep_seconds=100):
            disk_request = {"disk_space": disk_space}
            if medium_type is not None:
                disk_request["medium_name"] = medium_type
                if account is not None:
                    disk_request["account"] = account

            return map(
                command="cat; echo $(pwd) >&2; sleep {}".format(sleep_seconds),
                in_="//tmp/in",
                out="//tmp/out",
                spec={
                    "mapper": {
                        "disk_request": disk_request
                    },
                    "max_failed_job_count": 1,
                },
                track=track,
            )

        # Check that account is obligatory for ssd medium
        with pytest.raises(YtError):
            start_op("ssd", None, 1024 * 1024, track=True)

        start_op("ssd", "my_account", 1024 * 1024, track=True, sleep_seconds=1)

        # Artificially take some space from account.
        tx = start_transaction(timeout=60000)
        lease_id = create_account_resource_usage_lease(account="my_account", transaction_id=tx)
        set("#{}/@resource_usage".format(lease_id), {"disk_space_per_medium": {"ssd": 1024}})

        # Check that operation failed due to lack of space.
        with pytest.raises(YtError):
            start_op("ssd", "my_account", 1024 * 1024, track=True)

        abort_transaction(tx)

        op = start_op("ssd", "my_account", 768 * 1024, track=False)
        wait(lambda: op.get_running_jobs())

        # Check that second operation failed due to lack of space.
        with pytest.raises(YtError):
            start_op("ssd", "my_account", 768 * 1024, track=True)

    @authors("ignat")
    @pytest.mark.timeout(150)
    def test_disabled_accounting(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        create("table", "//tmp/out")

        create_account("my_account")
        set("//sys/accounts/my_account/@resource_limits/disk_space_per_medium/ssd", 1024 * 1024)

        update_controller_agent_config("enable_master_resource_usage_accounting", False)

        def start_op(medium_type, account, disk_space, track, sleep_seconds=100):
            disk_request = {"disk_space": disk_space}
            if medium_type is not None:
                disk_request["medium_name"] = medium_type
                if account is not None:
                    disk_request["account"] = account

            return map(
                command="cat; echo $(pwd) >&2; sleep {}".format(sleep_seconds),
                in_="//tmp/in",
                out="//tmp/out",
                spec={
                    "mapper": {
                        "disk_request": disk_request
                    },
                    "max_failed_job_count": 1,
                },
                track=track,
            )

        # Check that account is obligatory for ssd medium
        with pytest.raises(YtError):
            start_op("ssd", None, 1024 * 1024, track=True)

        start_op("ssd", "my_account", 1024 * 1024, track=True, sleep_seconds=1)

        # Artificially take some space from account.
        tx = start_transaction(timeout=60000)
        lease_id = create_account_resource_usage_lease(account="my_account", transaction_id=tx)
        set("#{}/@resource_usage".format(lease_id), {"disk_space_per_medium": {"ssd": 1024}})

        # Check that operation is successful since accounting is disabled.
        start_op("ssd", "my_account", 1024 * 1024, track=True)

    @authors("ignat")
    def test_enable_accounting_after_revive(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        create("table", "//tmp/out")

        create_account("my_account")
        set("//sys/accounts/my_account/@resource_limits/disk_space_per_medium/ssd", 1024 * 1024)

        update_controller_agent_config("enable_master_resource_usage_accounting", False)

        def start_op(medium_type, account, disk_space, track, sleep_seconds=100):
            disk_request = {"disk_space": disk_space}
            if medium_type is not None:
                disk_request["medium_name"] = medium_type
                if account is not None:
                    disk_request["account"] = account

            return map(
                command="cat; echo $(pwd) >&2; sleep {}".format(sleep_seconds),
                in_="//tmp/in",
                out="//tmp/out",
                spec={
                    "mapper": {
                        "disk_request": disk_request
                    },
                    "max_failed_job_count": 1,
                },
                track=track,
            )

        op = start_op("ssd", "my_account", 1024 * 1024, track=False)

        time.sleep(1)
        assert get("//sys/accounts/my_account/@resource_usage/disk_space_per_medium/ssd") == 0

        op.wait_for_fresh_snapshot()
        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            update_controller_agent_config("enable_master_resource_usage_accounting", True, wait_for_orchid=False)

        wait(lambda: op.get_state() == "running")

        time.sleep(1)
        assert get("//sys/accounts/my_account/@resource_usage/disk_space_per_medium/ssd") == 0

    @authors("ignat")
    def test_revive(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        create("table", "//tmp/out")

        create_account("my_account")
        set("//sys/accounts/my_account/@resource_limits/disk_space_per_medium/ssd", 1024 * 1024)

        def start_op(medium_type, account, disk_space, track, sleep_seconds=100):
            disk_request = {"disk_space": disk_space}
            if medium_type is not None:
                disk_request["medium_name"] = medium_type
                if account is not None:
                    disk_request["account"] = account

            return map(
                command="cat; echo $(pwd) >&2; sleep {}".format(sleep_seconds),
                in_="//tmp/in",
                out="//tmp/out",
                spec={
                    "mapper": {
                        "disk_request": disk_request
                    },
                    "max_failed_job_count": 1,
                },
                track=track,
            )

        op = start_op("ssd", "my_account", 1024 * 1024, track=False)

        wait(lambda: get("//sys/accounts/my_account/@resource_usage/disk_space_per_medium/ssd") == 1024 * 1024)

        op.wait_for_fresh_snapshot()
        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        wait(lambda: op.get_state() == "running")

        assert get("//sys/accounts/my_account/@resource_usage/disk_space_per_medium/ssd") == 1024 * 1024

    @authors("ignat")
    def test_revive_with_job_restart(self):
        create_account("my_account")
        set("//sys/accounts/my_account/@resource_limits/disk_space_per_medium/ssd", 1024 * 1024)

        def start_op(medium_type, account, disk_space, track, sleep_seconds=100):
            disk_request = {"disk_space": disk_space}
            if medium_type is not None:
                disk_request["medium_name"] = medium_type
                if account is not None:
                    disk_request["account"] = account

            return run_test_vanilla(
                command="sleep {}".format(sleep_seconds),
                task_patch={"disk_request": disk_request},
                spec={"max_failed_job_count": 1},
                track=track,
            )

        op = start_op("ssd", "my_account", 1024 * 1024, track=False)

        wait(lambda: get("//sys/accounts/my_account/@resource_usage/disk_space_per_medium/ssd") == 1024 * 1024)

        assert op.get_job_count("running") == 1
        op.wait_for_fresh_snapshot()
        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        wait(lambda: op.get_state() == "running")
        wait(lambda: op.get_job_count("running") == 1)

        jobs = list(op.get_running_jobs())
        assert len(jobs) == 1

        abort_job(jobs[0])
        wait(lambda: op.get_job_count("aborted") == 1)
        wait(lambda: op.get_job_count("running") == 1)

        wait(lambda: get("//sys/accounts/my_account/@resource_usage/disk_space_per_medium/ssd") == 1024 * 1024)

    @authors("ignat")
    def test_multiple_jobs(self):
        update_controller_agent_config("max_retained_jobs_per_operation", 2)

        create_account("my_account")
        set("//sys/accounts/my_account/@resource_limits/disk_space_per_medium/ssd", 1024 * 1024)

        disk_request = {"disk_space": 512 * 1024, "medium_name": "ssd", "account": "my_account"}
        op = run_test_vanilla("echo $JOB_INDEX >&2", job_count=5, spec={"max_stderr_count": 1}, task_patch={"disk_request": disk_request})
        op.track()

        assert get("//sys/accounts/my_account/@resource_usage/disk_space_per_medium/ssd") == 0
