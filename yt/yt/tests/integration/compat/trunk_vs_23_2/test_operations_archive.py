from yt_env_setup import YTEnvSetup
from yt_commands import (
    authors, run_test_vanilla, wait, exists,
    with_breakpoint, release_breakpoint, sync_create_cells,
    get_operation)

import yt.environment.init_operations_archive as init_operations_archive


##################################################################

class TestOperationsArchive(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 3

    MIN_OPERATIONS_ARCHIVE_VERSION = 48

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
        "trunk": [
            "master", "scheduler", "controller-agent",
            "proxy", "http-proxy", "node", "job-proxy", "exec", "tools"
        ],
    }

    FORCE_CREATE_ENVIRONMENT = True

    def setup_method(self, method):
        super(TestOperationsArchive, self).setup_method(method)
        sync_create_cells(1)
        init_operations_archive.create_tables(
            self.Env.create_native_client(),
            target_version=self.MIN_OPERATIONS_ARCHIVE_VERSION,
            override_tablet_cell_bundle="default",
            shard_count=1,
            archive_path=init_operations_archive.DEFAULT_ARCHIVE_PATH)

    @authors("ignat")
    def test(self):
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=1)

        wait(lambda: op.get_state() == "running")
        wait(lambda: len(op.get_running_jobs(verbose=True)) == 1)

        op.wait_for_fresh_snapshot()

        release_breakpoint()

        wait(lambda: op.get_state() == "completed")

        assert get_operation(op.id, attributes=["state"])["state"] == "completed"

        wait(lambda: not exists(op.get_path()))

        assert get_operation(op.id)["state"] == "completed"

        assert op.lookup_in_archive()["state"] == "completed"
