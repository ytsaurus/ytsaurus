from yt_env_setup import (YTEnvSetup, Restarter, CONTROLLER_AGENTS_SERVICE, SCHEDULERS_SERVICE)

from yt_commands import (
    authors, print_debug, vanilla, with_breakpoint, wait_breakpoint, release_breakpoint,
    get_allocation_id_from_job_id, run_test_vanilla, abort_job, create, write_file,
    update_nodes_dynamic_config, update_controller_agent_config, wait,
    sync_create_cells, get, ls, exists,
)

import yt.environment.init_operations_archive as init_operations_archive

import pytest
import time


##################################################################


class TestSeveralJobsInAllocation(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 200,
            "snapshot_writer": {
                "upload_replication_factor": 1,
                "min_upload_replication_factor": 1,
            },
        }
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 1,
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "allocation": {
                        "enable_multiple_jobs": True,
                    },
                },
            },
        },
    }

    @authors("pogorelov")
    @pytest.mark.parametrize("with_job_revival", [False, True])
    def test_vanilla_allocation_reused_within_task(self, with_job_revival):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 2,
                        "command": with_breakpoint("BREAKPOINT"),
                    },
                },
                "enable_multiple_jobs_in_allocation": True,
            },
        )

        job_id1, = wait_breakpoint(job_count=1)
        print_debug(f"job_id1: {job_id1}")

        if with_job_revival:
            op.wait_for_fresh_snapshot()

            with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
                pass

        release_breakpoint(job_id=job_id1)

        job_id2, = wait_breakpoint(job_count=1)
        print_debug(f"job_id2: {job_id2}")
        release_breakpoint(job_id=job_id2)

        print_debug(f"job_id1: {job_id1}, job_id2: {job_id2}")

        assert get_allocation_id_from_job_id(job_id1) == get_allocation_id_from_job_id(job_id2)

        op.track()

    @authors("pogorelov")
    def test_vanilla_allocation_not_reused_between_tasks(self):
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
                "enable_multiple_jobs_in_allocation": True,
            },
        )
        job_id1, = wait_breakpoint(job_count=1)
        release_breakpoint(job_id=job_id1)

        job_id2, = wait_breakpoint(job_count=1)
        release_breakpoint(job_id=job_id2)

        print_debug(f"job_id1: {job_id1}, job_id2: {job_id2}")

        assert get_allocation_id_from_job_id(job_id1) != get_allocation_id_from_job_id(job_id2)

        op.track()

    @authors("pogorelov")
    def test_reusing_many_times(self):
        job_count = 10
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "command": with_breakpoint("BREAKPOINT"),
                        "job_count": 10,
                    },
                },
                "enable_multiple_jobs_in_allocation": True,
            },
        )

        allocation_id = None

        for i in range(job_count):
            job_id, = wait_breakpoint(job_count=1)
            release_breakpoint(job_id=job_id)

            if allocation_id is None:
                allocation_id = get_allocation_id_from_job_id(job_id)
            else:
                assert get_allocation_id_from_job_id(job_id) == allocation_id

        op.track()


##################################################################


class TestArtifactReuseInAllocationBase(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "allocation": {
                        "enable_multiple_jobs": True,
                    },
                },
                "job_reporter": {
                    "reporting_period": 10,
                    "min_repeat_delay": 10,
                    "max_repeat_delay": 10,
                },
            },
        },
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
            "enable_job_stderr_reporter": True,
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "job_reporter": {
                "enabled": True,
                "reporting_period": 10,
                "min_repeat_delay": 10,
                "max_repeat_delay": 10,
            },
        },
    }

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        # Force artifact eviction by setting the artifact cache quota to 1 byte.
        # This ensures the second job cannot reuse the artifact via the artifact cache
        # and must rely solely on the TJobFSSecretary's stored TArtifactPtr.
        for location in config["data_node"]["cache_locations"]:
            location["quota"] = 1

    def setup_method(self, method):
        super(TestArtifactReuseInAllocationBase, self).setup_method(method)
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )


##################################################################


class TestArtifactReuseInAllocation(TestArtifactReuseInAllocationBase):
    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 1,
            },
        },
    }

    @authors("pogorelov")
    def test_artifacts_not_redownloaded_on_allocation_reuse(self):
        file_size = 1024
        create("file", "//tmp/test_file", attributes={"replication_factor": 1})
        write_file("//tmp/test_file", b"x" * file_size)

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 2,
                        "command": with_breakpoint("BREAKPOINT"),
                        "file_paths": ["//tmp/test_file"],
                    },
                },
                "enable_multiple_jobs_in_allocation": True,
                "max_failed_job_count": 1,
            },
        )

        job_id1, = wait_breakpoint(job_count=1)
        release_breakpoint(job_id=job_id1)

        job_id2, = wait_breakpoint(job_count=1)

        assert get_allocation_id_from_job_id(job_id1) == get_allocation_id_from_job_id(job_id2)

        release_breakpoint(job_id=job_id2)

        op.track()

        stats1 = op.get_job_statistics(job_id1)
        stats2 = op.get_job_statistics(job_id2)

        # First job must have downloaded the artifact.
        assert stats1["exec_agent"]["artifacts"]["cache_miss_artifacts_size"]["sum"] > 0
        # Second job in same allocation must not have downloaded anything.
        assert stats2["exec_agent"]["artifacts"]["cache_miss_artifacts_size"]["sum"] == 0
        assert stats2["exec_agent"]["artifacts"]["cache_hit_artifacts_size"]["sum"] == 0

    @authors("pogorelov")
    def test_artifact_evicted_when_allocation_not_reused(self):
        create("file", "//tmp/test_file", attributes={"replication_factor": 1})
        write_file("//tmp/test_file", b"x" * 1024)

        update_controller_agent_config("allocation_job_count_limit", 1)

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 2,
                        "command": with_breakpoint("BREAKPOINT"),
                        "file_paths": ["//tmp/test_file"],
                    },
                },
                "enable_multiple_jobs_in_allocation": True,
                "max_failed_job_count": 1,
                "testing": {
                    "settle_job_delay": {
                        "duration": 2000,
                        "type": "async",
                    },
                },
            },
        )

        job_id1, = wait_breakpoint(job_count=1)
        release_breakpoint(job_id=job_id1)

        job_id2, = wait_breakpoint(job_count=1)
        release_breakpoint(job_id=job_id2)

        op.track()

        assert get_allocation_id_from_job_id(job_id1) != get_allocation_id_from_job_id(job_id2)

        stats1 = op.get_job_statistics(job_id1)
        stats2 = op.get_job_statistics(job_id2)

        # With allocation_job_count_limit=1 the allocation is not reused, so artifact
        # is released and immediately evicted (quota=1). Both jobs must re-download.
        assert stats1["exec_agent"]["artifacts"]["cache_miss_artifacts_size"]["sum"] > 0
        assert stats2["exec_agent"]["artifacts"]["cache_miss_artifacts_size"]["sum"] > 0

    @authors("pogorelov")
    def test_bypassed_artifact_not_reused(self):
        create("file", "//tmp/test_file", attributes={"replication_factor": 1})
        write_file("//tmp/test_file", b"x" * 1024)

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 2,
                        "command": with_breakpoint("BREAKPOINT"),
                        "file_paths": ["<bypass_artifact_cache=%true>//tmp/test_file"],
                    },
                },
                "enable_multiple_jobs_in_allocation": True,
                "max_failed_job_count": 1,
            },
        )

        job_id1, = wait_breakpoint(job_count=1)
        release_breakpoint(job_id=job_id1)

        job_id2, = wait_breakpoint(job_count=1)
        release_breakpoint(job_id=job_id2)

        op.track()

        assert get_allocation_id_from_job_id(job_id1) == get_allocation_id_from_job_id(job_id2)

        stats1 = op.get_job_statistics(job_id1)
        stats2 = op.get_job_statistics(job_id2)

        # Bypass artifacts are always re-read from the source, not from cache.
        assert stats1["exec_agent"]["artifacts"]["cache_bypassed_artifacts_size"]["sum"] > 0
        assert stats2["exec_agent"]["artifacts"]["cache_bypassed_artifacts_size"]["sum"] > 0

##################################################################


class TestArtifactReuseInAllocationOfGangOperation(TestArtifactReuseInAllocationBase):
    NUM_NODES = 2

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 1,
            },
        },
    }

    @authors("pogorelov")
    def test_artifacts_not_redownloaded_in_gang_operation(self):
        create("file", "//tmp/test_file", attributes={"replication_factor": 1})
        write_file("//tmp/test_file", b"x" * 1024)

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=2,
            task_patch={
                "gang_options": {},
                "file_paths": ["//tmp/test_file"],
            },
            spec={
                "enable_multiple_jobs_in_allocation": True,
                "max_failed_job_count": 1,
            },
            track=False,
        )

        job_ids = wait_breakpoint(job_count=2)

        first_job_id = job_ids[0]

        first_incarnation_reused_job_id = next(jid for jid in job_ids if jid != first_job_id)
        reused_allocation_id = get_allocation_id_from_job_id(first_incarnation_reused_job_id)

        current_incarnation = get(op.get_orchid_path() + "/controller/operation_incarnation")

        abort_job(first_job_id)

        wait(lambda: get(op.get_orchid_path() + "/controller/operation_incarnation") != current_incarnation)

        for job_id in job_ids:
            release_breakpoint(job_id=job_id)

        new_job_ids = wait_breakpoint(job_count=2)
        release_breakpoint()

        op.track()

        new_job_ids_in_reused_alloc = [
            jid for jid in new_job_ids
            if get_allocation_id_from_job_id(jid) == reused_allocation_id
        ]
        assert len(new_job_ids_in_reused_alloc) == 1

        first_stats = op.get_job_statistics(first_incarnation_reused_job_id)
        assert first_stats["exec_agent"]["artifacts"]["cache_miss_artifacts_size"]["sum"] > 0

        reused_job_stats = op.get_job_statistics(new_job_ids_in_reused_alloc[0])
        assert reused_job_stats["exec_agent"]["artifacts"]["cache_miss_artifacts_size"]["sum"] == 0
        assert reused_job_stats["exec_agent"]["artifacts"]["cache_hit_artifacts_size"]["sum"] == 0

    @authors("pogorelov")
    def test_gang_jobs_aborted_before_artifact_download(self):
        create("file", "//tmp/test_file", attributes={"replication_factor": 1})
        write_file("//tmp/test_file", b"x" * 1024)

        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "testing": {
                            "delay_in_artifacts_caching": 30000,
                        },
                    },
                },
            },
        })

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=2,
            task_patch={
                "gang_options": {},
                "file_paths": ["//tmp/test_file"],
            },
            spec={
                "enable_multiple_jobs_in_allocation": True,
                "max_failed_job_count": 1,
            },
            track=False,
        )

        # NB(pogorelov): We can not use wait_breakpoint here because it set only after user code is launched.
        wait(lambda: len(op.list_jobs()) == 2)
        first_incarnation_job_ids = list(op.list_jobs())
        job_id = first_incarnation_job_ids[0]
        wait(lambda: op.get_job_phase(job_id) == "caching_artifacts")

        time.sleep(3)

        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "testing": {
                            "delay_in_artifacts_caching": None,
                        },
                    },
                },
            },
        })

        first_incarnation_reused_job_id = next(
            jid for jid in first_incarnation_job_ids if jid != job_id
        )
        reused_allocation_id = get_allocation_id_from_job_id(first_incarnation_reused_job_id)

        current_incarnation = get(op.get_orchid_path() + "/controller/operation_incarnation")

        abort_job(job_id)

        wait(lambda: get(op.get_orchid_path() + "/controller/operation_incarnation") != current_incarnation)

        new_job_ids = wait_breakpoint(job_count=2)
        release_breakpoint()

        op.track()

        new_job_ids_in_reused_alloc = [
            jid for jid in new_job_ids
            if get_allocation_id_from_job_id(jid) == reused_allocation_id
        ]
        assert len(new_job_ids_in_reused_alloc) == 1

        # New incarnation job in the same allocation must re-download the artifact:
        # ArtifactsCached_ was not set (abort happened during the delay, before caching completed).
        reused_job_stats = op.get_job_statistics(new_job_ids_in_reused_alloc[0])
        assert reused_job_stats["exec_agent"]["artifacts"]["cache_miss_artifacts_size"]["sum"] > 0


##################################################################


class TestSuspendedOperationDoesNotSettleJobs(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 200,
            "snapshot_writer": {
                "upload_replication_factor": 1,
                "min_upload_replication_factor": 1,
            },
        }
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 1,
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "allocation": {
                        "enable_multiple_jobs": True,
                    },
                },
            },
        },
    }

    def _get_orchid_paths(self):
        controller_agent_address = ls("//sys/controller_agents/instances")[0]
        base = f"//sys/controller_agents/instances/{controller_agent_address}/orchid/controller_agent/job_tracker"
        return (
            base + "/allocations",
            base + "/operations",
        )

    @authors("pogorelov")
    def test_suspended_operation_does_not_settle_jobs(self):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 2,
                        "command": with_breakpoint("BREAKPOINT"),
                    },
                },
                "enable_multiple_jobs_in_allocation": True,
            },
        )

        allocations_orchid_path, _ = self._get_orchid_paths()

        job_id1, = wait_breakpoint(job_count=1)
        allocation_id = get_allocation_id_from_job_id(job_id1)

        assert exists(allocations_orchid_path + f"/{allocation_id}")

        op.suspend()
        wait(lambda: get(op.get_path() + "/@suspended"))

        release_breakpoint(job_id=job_id1)
        wait(lambda: op.get_job_count("completed") == 1)

        wait(lambda: not exists(allocations_orchid_path + f"/{allocation_id}"))

        op.resume()
        wait(lambda: not get(op.get_path() + "/@suspended"))

        job_id2, = wait_breakpoint(job_count=1)
        release_breakpoint(job_id=job_id2)

        op.track()

        # After resume, job2 must run in a NEW allocation.
        assert get_allocation_id_from_job_id(job_id2) != allocation_id

    @authors("pogorelov")
    @pytest.mark.parametrize("service_to_restart", [CONTROLLER_AGENTS_SERVICE, SCHEDULERS_SERVICE])
    def test_suspended_operation_does_not_settle_jobs_after_revival(self, service_to_restart):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 2,
                        "command": with_breakpoint("BREAKPOINT"),
                    },
                },
                "enable_multiple_jobs_in_allocation": True,
            },
        )

        job_id1, = wait_breakpoint(job_count=1)
        allocation_id = get_allocation_id_from_job_id(job_id1)

        op.suspend()
        wait(lambda: get(op.get_path() + "/@suspended"))

        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, service_to_restart):
            pass

        allocations_orchid_path, operations_orchid_path = self._get_orchid_paths()

        wait(lambda: exists(allocations_orchid_path + f"/{allocation_id}"))

        assert get(operations_orchid_path + f"/{op.id}/suspended")

        release_breakpoint(job_id=job_id1)
        wait(lambda: op.get_job_count("completed") == 1)

        wait(lambda: not exists(allocations_orchid_path + f"/{allocation_id}"))

        op.resume()
        wait(lambda: not get(op.get_path() + "/@suspended"))

        job_id2, = wait_breakpoint(job_count=1)
        release_breakpoint(job_id=job_id2)

        op.track()

        # After resume, job2 must run in a NEW allocation.
        assert get_allocation_id_from_job_id(job_id2) != allocation_id
