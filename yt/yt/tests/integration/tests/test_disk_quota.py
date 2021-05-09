from yt_env_setup import wait, YTEnvSetup
from yt_commands import *

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
        "exec_agent": {
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
                "test_disk_quota": True,
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
        config["exec_agent"]["slot_manager"]["locations"][0]["path"] = cls.fake_default_disk_path

    def _init_tables(self):
        tables = ["//tmp/t1", "//tmp/t2", "//tmp/t3"]
        for table in tables:
            create("table", table)
        write_table(tables[0], [{"foo": "bar"} for _ in xrange(10)])
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
                "mapper": {"disk_space_limit": 1024 * 1024 / 2},
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
                "mapper": {"disk_space_limit": 1024 * 1024 * 2 / 3},
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

        time.sleep(1.0)

        # NB: We have no sanity checks for disk space in scheduler
        for type in ("running", "aborted", "failed"):
            assert op.get_job_count(type) == 0

    @authors("ignat")
    def test_scheduled_after_wait(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")
        write_table("//tmp/t1", [{"foo": "bar"}])

        op1 = map(
            track=False,
            command="sleep 1000",
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "mapper": {"disk_space_limit": 2 * 1024 * 1024 / 3},
                "max_failed_job_count": 1,
            },
        )
        op1.ensure_running()
        wait(lambda: op1.get_job_count("running") == 1)

        op2 = map(
            track=False,
            command="sleep 1000",
            in_="//tmp/t1",
            out="//tmp/t3",
            spec={
                "mapper": {"disk_space_limit": 2 * 1024 * 1024 / 3},
                "max_failed_job_count": 1,
            },
        )
        op2.ensure_running()
        for type in ("running", "aborted", "failed"):
            assert op2.get_job_count(type) == 0

        op1.abort()

        wait(lambda: op2.get_job_count("running") == 1)
        op2.abort()


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
        "exec_agent": {
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
                "test_disk_quota": True,
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

        config["exec_agent"]["slot_manager"]["locations"] = [
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
        create_medium("ssd")

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

        jobs = ls(op.get_path() + "/jobs")
        assert len(jobs) == 1
        assert op.read_stderr(jobs[0]).startswith(self.fake_ssd_disk_path)

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
        set("//sys/cluster_nodes/{0}/@resource_limits_overrides/cpu".format(node), 0)

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

        set("//sys/cluster_nodes/{0}/@resource_limits_overrides/cpu".format(node), 3.0)

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
        "exec_agent": {
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
                "test_disk_quota": True,
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

        config["exec_agent"]["slot_manager"]["locations"] = [
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
        create_medium("ssd")

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
        "exec_agent": {
            "slot_manager": {
                "disk_resources_update_period": 100,
            },
            "job_controller": {"resource_limits": {"user_slots": 1, "cpu": 1.0}},
            "min_required_disk_space": 0,
        },
        "data_node": {
            "volume_manager": {
                "test_disk_quota": True,
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

        config["exec_agent"]["slot_manager"]["locations"] = [
            {
                "path": cls.fake_ssd_disk_path,
                "disk_quota": 2 * 1024 * 1024,
                "disk_usage_watermark": 0,
                "medium_name": "ssd",
            }
        ]
        config["exec_agent"]["slot_manager"]["default_medium_name"] = "hdd"

    @classmethod
    def on_masters_started(cls):
        create_medium("hdd")
        create_medium("ssd")

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
        "exec_agent": {
            "slot_manager": {
                "disk_resources_update_period": 100,
            },
            "job_controller": {"resource_limits": {"user_slots": 3, "cpu": 3.0}},
            "min_required_disk_space": 0,
        },
        "data_node": {
            "volume_manager": {
                "test_disk_quota": True,
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

        config["exec_agent"]["slot_manager"]["locations"] = [
            {
                "path": cls.fake_ssd_disk_path,
                "disk_quota": 2 * 1024 * 1024,
                "disk_usage_watermark": 0,
                "medium_name": "ssd",
            }
        ]
        config["exec_agent"]["slot_manager"]["default_medium_name"] = "ssd"

    @classmethod
    def on_masters_started(cls):
        create_medium("hdd")
        create_medium("ssd")

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

        wait(lambda: sum([op.get_job_count("running", verbose=True) for op in (op1, op2, op3)]) == 2)

        assert op1.get_job_count("aborted") == 0
        assert op2.get_job_count("aborted") == 0
        assert op3.get_job_count("aborted") == 0


class TestDefaultDiskMediumWithUnspecifiedMediumAndMultipleSlotsPorto(YTEnvSetup, DiskMediumTestConfiguration):
    NUM_SCHEDULERS = 1
    NUM_MASTERS = 1
    NUM_NODES = 1

    DELTA_MASTER_CONFIG = DiskMediumTestConfiguration.MASTER_CONFIG
    DELTA_SCHEDULER_CONFIG = DiskMediumTestConfiguration.SCHEDULER_CONFIG
    DELTA_CONTROLLER_AGENT_CONFIG = DiskMediumTestConfiguration.CONTROLLER_AGENT_CONFIG
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "slot_manager": {
                "disk_resources_update_period": 100,
            },
            "job_controller": {"resource_limits": {"user_slots": 1, "cpu": 1.0}},
            "min_required_disk_space": 0,
        },
        "data_node": {
            "volume_manager": {
                "test_disk_quota": True,
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

        config["exec_agent"]["slot_manager"]["locations"] = [
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
        config["exec_agent"]["slot_manager"]["default_medium_name"] = "ssd"

    @classmethod
    def on_masters_started(cls):
        create_medium("hdd")
        create_medium("ssd")

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
        jobs = op.get_running_jobs().keys()
        assert len(nodes) == 1
        assert len(jobs) == 1

        node_orchid_job_path = "//sys/cluster_nodes/{}/orchid/job_controller/active_jobs/scheduler/{}".format(nodes[0], jobs[0])
        wait(lambda: exists(node_orchid_job_path))
        wait(lambda: get(node_orchid_job_path + "/exec_attributes/medium_name") == "ssd")
