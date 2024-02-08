from yt_env_setup import YTEnvSetup, Restarter, SCHEDULERS_SERVICE, CONTROLLER_AGENTS_SERVICE
from yt_commands import (
    authors, print_debug, run_test_vanilla, wait, exists,
    with_breakpoint, release_breakpoint, sync_create_cells,
    get_operation)


import yt.environment.init_operations_archive as init_operations_archive

import yatest.common

import os
import pytest

##################################################################


def replace_binaries_with_trunk_version(bin_path):
    scheduler_path = os.path.join(bin_path, "ytserver-scheduler")
    controller_agent_path = os.path.join(bin_path, "ytserver-controller-agent")
    ytserver_all_trunk_path = yatest.common.binary_path("yt/yt/packages/tests_package/ytserver-all")

    print_debug("Removing {} and {}".format(scheduler_path, controller_agent_path))
    os.remove(scheduler_path)
    os.remove(controller_agent_path)

    print_debug("Symlinking {} to {}".format(ytserver_all_trunk_path, scheduler_path))
    os.symlink(ytserver_all_trunk_path, scheduler_path)

    print_debug("Symlinking {} to {}".format(ytserver_all_trunk_path, controller_agent_path))
    os.symlink(ytserver_all_trunk_path, controller_agent_path)


def check_running_operation(lookup_in_archive=False):
    op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=1)

    wait(lambda: op.get_state() == "running")
    wait(lambda: len(op.get_running_jobs(verbose=True)) == 1)
    job_ids = op.get_running_jobs().keys()

    op.wait_for_fresh_snapshot()

    yield

    wait(lambda: op.get_state() == "running")

    # Clean start
    wait(lambda: op.get_running_jobs(verbose=True).keys() != job_ids)

    release_breakpoint()

    wait(lambda: op.get_state() == "completed")

    # Check clean start in events.
    found_pending = False
    events = get_operation(op.id, attributes=["events"])["events"]
    for event in reversed(events):
        if event["state"] == "pending":
            assert "attributes" in event
            assert "revived_from_snapshot" in event["attributes"]
            assert not event["attributes"]["revived_from_snapshot"]
            found_pending = True
            break
    assert found_pending

    if lookup_in_archive:
        wait(lambda: not exists(op.get_path()))
        assert op.lookup_in_archive()["state"] == "completed"


class TestSchedulerUpdate(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 3

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
        }
    }

    ARTIFACT_COMPONENTS = {
        "23_2": ["master", "scheduler", "controller-agent"],
        "trunk": ["proxy", "http-proxy", "node", "job-proxy", "exec", "tools"],
    }

    FORCE_CREATE_ENVIRONMENT = True

    @authors("ignat")
    def test(self):
        # TODO(max42, galtsev)
        pytest.skip("Test is disabled due to crashes in CA")

        CHECKER_LIST = [check_running_operation]

        checker_state_list = [iter(c()) for c in CHECKER_LIST]
        for s in checker_state_list:
            next(s)

        with Restarter(self.Env, [SCHEDULERS_SERVICE, CONTROLLER_AGENTS_SERVICE]):
            replace_binaries_with_trunk_version(self.bin_path)

        for s in checker_state_list:
            with pytest.raises(StopIteration):
                next(s)


class TestSchedulerUpdateWithOperationsCleaner(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 3

    USE_DYNAMIC_TABLES = True

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operations_cleaner": {
                "enable": True,
                # Analyze all operations each 100ms
                "analysis_period": 100,
                # Operations older than 50ms can be considered for removal
                "clean_delay": 50,
                # If more than this count of operations are enqueued and archivation
                # can't succeed then operations will be just removed.
                "max_operation_count_enqueued_for_archival": 5,
                "max_operation_count_per_user": 0,
            }
        }
    }

    ARTIFACT_COMPONENTS = {
        "23_2": ["master", "scheduler", "controller-agent"],
        "trunk": ["proxy", "http-proxy", "node", "job-proxy", "exec", "tools"],
    }

    FORCE_CREATE_ENVIRONMENT = True

    def setup_method(self, method):
        super(TestSchedulerUpdateWithOperationsCleaner, self).setup_method(method)
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )

    @authors("ignat")
    def test(self):
        # TODO(max42, galtsev)
        pytest.skip("Test is disabled due to crashes in CA")

        CHECKER_LIST = [lambda: check_running_operation(lookup_in_archive=True)]

        checker_state_list = [iter(c()) for c in CHECKER_LIST]
        for s in checker_state_list:
            next(s)

        with Restarter(self.Env, [SCHEDULERS_SERVICE, CONTROLLER_AGENTS_SERVICE]):
            replace_binaries_with_trunk_version(self.bin_path)

        for s in checker_state_list:
            with pytest.raises(StopIteration):
                next(s)
