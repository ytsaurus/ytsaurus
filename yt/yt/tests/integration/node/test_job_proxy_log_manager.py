from yt_env_setup import (YTEnvSetup, Restarter, NODES_SERVICE)

from yt_commands import (
    run_test_vanilla, with_breakpoint, wait_breakpoint, authors, release_breakpoint,
    update_nodes_dynamic_config, wait, update, print_debug, remove, create,
    dump_job_proxy_log, read_file, sync_create_cells,
)

import yt.environment.init_operations_archive as init_operations_archive

from yt.common import YtError

import pytest

import os.path


def log_dir_from_job_id(path_to_run, job_id):
    sharding_key = job_id.split("-")[0]
    if len(sharding_key) < 8:
        sharding_key = "0"
    else:
        sharding_key = sharding_key[0]

    return os.path.join(
        path_to_run,
        "logs/job_proxy-0",
        sharding_key,
        job_id,
    )


def verify_job_proxy_logs_exist(path_to_run, job_id):
    log_dir = log_dir_from_job_id(path_to_run, job_id)

    files = os.listdir(log_dir)

    print_debug(f"Files in log dir {log_dir} are: ", files)

    assert len(files) >= 2
    assert "job_proxy-0.debug.log" in files
    assert "job_proxy-0.log" in files


class TestJobProxyLogging(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    JOB_PROXY_LOGGING = {"mode": "per_job_directory"}

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_controller": {
                "resource_limits": {
                    "user_slots": 5,
                    "cpu": 5,
                    "memory": 5 * 1024 ** 3,
                },
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 5,
                "cpu": 5,
                "memory": 5 * 1024 ** 3,
            },
        },
    }

    @authors("tagirhamitov", "pogorelov")
    def test_separate_directory(self):
        job_count = 3

        run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=job_count)
        job_ids = wait_breakpoint(job_count=job_count)
        assert len(job_ids) == job_count

        for job_id in job_ids:
            verify_job_proxy_logs_exist(self.path_to_run, job_id)


class TestJobProxyLogManagerBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    JOB_PROXY_LOGGING = {"mode": "per_job_directory"}

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_controller": {
                "resource_limits": {
                    "user_slots": 5,
                    "cpu": 5,
                    "memory": 5 * 1024 ** 3,
                },
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 5,
                "cpu": 5,
                "memory": 5 * 1024 ** 3,
            },
        },
    }

    def check_job_proxy_log_dir_exists(self, job_id):
        log_dir = log_dir_from_job_id(self.path_to_run, job_id)

        print_debug(f"Checking job proxy log dir {log_dir} exists")

        return os.path.exists(log_dir)


class TestJobProxyLogManager(TestJobProxyLogManagerBase):
    DELTA_NODE_CONFIG = update(
        TestJobProxyLogManagerBase.DELTA_NODE_CONFIG,
        {
            "exec_node": {
                "job_proxy_log_manager": {
                    "logs_storage_period": "1s",
                },
            },
        }
    )

    @authors("tagirhamitov", "pogorelov")
    def test_removing_logs(self):
        job_count = 3

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=job_count)
        job_ids = wait_breakpoint(job_count=job_count)
        assert len(job_ids) == job_count

        for job_id in job_ids:
            verify_job_proxy_logs_exist(self.path_to_run, job_id)

        release_breakpoint()

        op.track()

        for job_id in job_ids:
            wait(lambda: not self.check_job_proxy_log_dir_exists(job_id))

    @authors("tagirhamitov", "pogorelov")
    def test_removing_logs_on_start(self):
        job_count = 3

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=job_count)
        job_ids = wait_breakpoint(job_count=job_count)
        assert len(job_ids) == job_count
        release_breakpoint()

        op.track()

        for job_id in job_ids:
            verify_job_proxy_logs_exist(self.path_to_run, job_id)

        with Restarter(self.Env, NODES_SERVICE):
            pass

        for job_id in job_ids:
            wait(lambda: not self.check_job_proxy_log_dir_exists(job_id))


class TestJobProxyLogManagerDynamicConfig(TestJobProxyLogManagerBase):
    @authors("tagirhamitov", "pogorelov")
    def test_dynamic_config(self):
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        job_id_1 = wait_breakpoint()[0]
        release_breakpoint(job_id=job_id_1)
        op.track()

        verify_job_proxy_logs_exist(self.path_to_run, job_id_1)

        update_nodes_dynamic_config({
            "exec_node": {
                "job_proxy_log_manager": {
                    "logs_storage_period": "1s",
                },
            },
        })

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        job_id_2 = wait_breakpoint()[0]
        verify_job_proxy_logs_exist(self.path_to_run, job_id_2)
        release_breakpoint(job_id=job_id_2)
        op.track()

        print_debug(f"Waiting for {job_id_2} job proxy log removal")

        wait(lambda: not self.check_job_proxy_log_dir_exists(job_id_2))
        verify_job_proxy_logs_exist(self.path_to_run, job_id_1)


class TestDumpJobProxyLog(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1
    USE_DYNAMIC_TABLES = True

    JOB_PROXY_LOGGING = {"mode": "per_job_directory"}

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_controller": {
                "resource_limits": {
                    "user_slots": 5,
                    "cpu": 5,
                    "memory": 5 * 1024 ** 3,
                },
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 5,
                "cpu": 5,
                "memory": 5 * 1024 ** 3,
            },
        },
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "operations_cleaner": {
                "enable": False,
                "analysis_period": 100,
                # Cleanup all operations
                "hard_retained_operation_count": 0,
                "clean_delay": 0,
            },
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
            "enable_job_stderr_reporter": True,
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_reporter": {
                    "reporting_period": 10,
                    "min_repeat_delay": 10,
                    "max_repeat_delay": 10,
                },
            },
        },
    }

    def setup_method(self, method):
        super(TestDumpJobProxyLog, self).setup_method(method)
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )

    def teardown_method(self, method):
        remove("//sys/operations_archive", force=True)
        super(TestDumpJobProxyLog, self).teardown_method(method)

    def validate_dumped_logs(self, path, job_id):
        for line in read_file(path).decode("utf-8").split("\n"):
            if "Job spec received" in line:
                assert job_id in line

    @authors("tagirhamitov", "pogorelov")
    def test_dump_for_running_job(self):
        path = "//tmp/job_proxy.log"
        create("file", path)

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        job_id = wait_breakpoint()[0]
        dump_job_proxy_log(job_id, op.id, path)
        release_breakpoint()
        op.track()

        self.validate_dumped_logs(path, job_id)

    @authors("tagirhamitov", "pogorelov")
    def test_dump_for_finished_job(self):
        path = "//tmp/job_proxy.log"
        create("file", path)

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        job_id = wait_breakpoint()[0]
        release_breakpoint()
        op.track()

        dump_job_proxy_log(job_id, op.id, path)
        self.validate_dumped_logs(path, job_id)

    @authors("tagirhamitov", "pogorelov")
    def test_dump_for_missing_job(self):
        update_nodes_dynamic_config({
            "exec_node": {
                "job_proxy_log_manager": {
                    "logs_storage_period": "0s",
                },
            },
        })
        path = "//tmp/job_proxy.log"
        create("file", path)

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        job_id = wait_breakpoint()[0]
        release_breakpoint()
        op.track()

        log_path = log_dir_from_job_id(self.path_to_run, job_id)
        wait(lambda: not os.path.exists(log_path))

        with pytest.raises(YtError, match="Job directory is not found"):
            dump_job_proxy_log(job_id, op.id, path)
