from yt_env_setup import (YTEnvSetup, Restarter, NODES_SERVICE)

from yt_commands import (
    run_test_vanilla, with_breakpoint, wait_breakpoint, authors, release_breakpoint,
    update_nodes_dynamic_config, wait, update, print_debug, remove, create,
    dump_job_proxy_log, read_file, sync_create_cells, ls, get
)

import yt.environment.init_operations_archive as init_operations_archive

from yt.common import YtError, WaitFailed

import pytest

import datetime
import os
import time


JOB_PROXY_ARTIFACTS = sorted([
    "job_proxy-0.debug.log",
    "job_proxy-0.log",
    "job_proxy-0-stderr",
    "ytserver_exec-0-stderr",
])


def get_sharding_key_from_job_id(job_id, key_length):
    sharding_key = job_id.split("-")[0]

    return sharding_key.zfill(8)[:key_length]


def log_dir_from_job_id(locations, job_id, sharding_key_length=1):
    sharding_key = get_sharding_key_from_job_id(job_id, sharding_key_length)

    return [
        os.path.join(storage, sharding_key, job_id)
        for storage in locations
    ]


def verify_symlink_job_dir(node_address, job_id, sharding_key_length):
    orchid_request = f"//sys/exec_nodes/{node_address}/orchid/config/exec_node/job_proxy_log_manager/job_proxy_log_symlinks_path"
    base_log_dir = get(orchid_request)

    key = get_sharding_key_from_job_id(job_id, sharding_key_length)

    artifacts = os.listdir(os.path.join(base_log_dir, key, job_id))
    for filename in JOB_PROXY_ARTIFACTS:
        assert filename in artifacts, f"Not found expected artifact in symlink job directory: {filename}"


def verify_job_proxy_logs_exist(locations, job_id, sharding_key_length=1):
    log_dirs = log_dir_from_job_id(locations, job_id, sharding_key_length)

    log_dir = None
    for candidate in log_dirs:
        if os.path.exists(candidate):
            log_dir = candidate
            break

    assert log_dir is not None, (
        f"Not found directory for job {job_id}"
    )

    files = os.listdir(log_dir)

    print_debug(f"Files in log dir {log_dir} are: ", files)

    for filename in JOB_PROXY_ARTIFACTS:
        filepath = os.path.join(log_dir, filename)

        assert os.path.isfile(filepath), (
            f"Expected log file {filename!r} not found for job_id={job_id!r}.\n"
            f"Job directory: {log_dir}",
        )


def get_locations():
    nodes = ls("//sys/exec_nodes")

    locations = set()
    for node_address in nodes:
        orchid_locations_request = f"//sys/exec_nodes/{node_address}/orchid/config/exec_node/job_proxy_log_manager/locations"
        locations |= {location["path"] for location in get(orchid_locations_request)}

    return locations


def no_alive_location_alert(node_address):
    alerts = get(f"//sys/exec_nodes/{node_address}/@alerts")
    for alert in alerts:
        if alert["message"] == "All job proxy log locations are disabled":
            return True

    return False


def job_completely_removed(job_id, node_address):
    active_jobs = get(f"//sys/exec_nodes/{node_address}/orchid/exec_node/job_controller/active_jobs")
    if str(job_id) in active_jobs:
        return False

    jobs_waiting = get(
        f"//sys/exec_nodes/{node_address}/orchid/exec_node/job_controller/jobs_waiting_for_cleanup"
    )

    return str(job_id) not in jobs_waiting


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

    @authors("epsilond1", "pogorelov")
    def test_separate_directory(self):
        job_count = 3

        run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=job_count)
        job_ids = wait_breakpoint(job_count=job_count)
        assert len(job_ids) == job_count

        for job_id in job_ids:
            verify_job_proxy_logs_exist(get_locations(), job_id)
            verify_symlink_job_dir(
                node_address=ls("//sys/exec_nodes")[0],
                sharding_key_length=1,
                job_id=job_id,
            )


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

    def check_job_proxy_log_dir_exists(self, job_id, sharding_key_length=1):
        log_dirs = log_dir_from_job_id(get_locations(), job_id, sharding_key_length)

        print_debug(f"Checking job proxy log dirs {log_dirs} storages exist")

        return any(os.path.exists(log_dir) for log_dir in log_dirs)


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

    @authors("epsilond1", "pogorelov")
    def test_removing_logs(self):
        job_count = 3

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=job_count)
        job_ids = wait_breakpoint(job_count=job_count)
        assert len(job_ids) == job_count

        for job_id in job_ids:
            verify_job_proxy_logs_exist(get_locations(), job_id)

        release_breakpoint()

        op.track()

        for job_id in job_ids:
            wait(lambda: not self.check_job_proxy_log_dir_exists(job_id))

    @authors("epsilond1", "pogorelov")
    def test_removing_logs_on_start(self):
        job_count = 3

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=job_count)
        job_ids = wait_breakpoint(job_count=job_count)
        assert len(job_ids) == job_count
        release_breakpoint()

        op.track()

        with Restarter(self.Env, NODES_SERVICE):
            pass

        for job_id in job_ids:
            wait(lambda: not self.check_job_proxy_log_dir_exists(job_id))


class TestJobProxyLogManagerDynamicConfig(TestJobProxyLogManagerBase):
    @authors("epsilond1", "pogorelov")
    def test_dynamic_config(self):
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        job_id_1 = wait_breakpoint()[0]
        release_breakpoint(job_id=job_id_1)
        op.track()

        verify_job_proxy_logs_exist(get_locations(), job_id_1)

        node_address = ls("//sys/exec_nodes")[0]

        wait(lambda: job_completely_removed(job_id_1, node_address) is True)

        update_nodes_dynamic_config({
            "exec_node": {
                "job_proxy_log_manager": {
                    "logs_storage_period": "1s",
                },
            },
        })

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        job_id_2 = wait_breakpoint()[0]
        verify_job_proxy_logs_exist(get_locations(), job_id_2)
        release_breakpoint(job_id=job_id_2)
        op.track()

        print_debug(f"Waiting for {job_id_2} job proxy log removal")
        wait(lambda: not self.check_job_proxy_log_dir_exists(job_id_2))

        verify_job_proxy_logs_exist(get_locations(), job_id_1)


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

    @authors("epsilond1", "pogorelov")
    def test_dump_for_running_job(self):
        path = "//tmp/job_proxy.log"
        create("file", path)

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        job_id = wait_breakpoint()[0]
        dump_job_proxy_log(job_id, op.id, path)
        release_breakpoint()
        op.track()

        self.validate_dumped_logs(path, job_id)

    @authors("epsilond1", "pogorelov")
    def test_dump_for_finished_job(self):
        path = "//tmp/job_proxy.log"
        create("file", path)

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        job_id = wait_breakpoint()[0]
        release_breakpoint()
        op.track()

        dump_job_proxy_log(job_id, op.id, path)
        self.validate_dumped_logs(path, job_id)

    @authors("epsilond1", "pogorelov")
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

        log_dirs = log_dir_from_job_id(get_locations(), job_id)
        wait(lambda: {os.path.exists(log_dir) for log_dir in log_dirs} == {False})

        with pytest.raises(YtError, match="Job directory is not found"):
            dump_job_proxy_log(job_id, op.id, path)


class TestJobProxyLogManagerLocationFailure(TestJobProxyLogManagerBase):
    DELTA_NODE_CONFIG = update(
        TestJobProxyLogManagerBase.DELTA_NODE_CONFIG,
        {
            "exec_node": {
                "job_proxy_log_manager": {
                    "location_check_period": "1s",
                },
            },
        }
    )

    def teardown_method(self, method):
        found_disabled = False
        for location in get_locations():
            disabled_path = os.path.join(location, "disabled")
            if os.path.exists(disabled_path):
                os.remove(disabled_path)
                found_disabled = True

        if found_disabled:
            with Restarter(self.Env, NODES_SERVICE):
                pass

        super().teardown_method(method)

    @authors("epsilond1", "pogorelov")
    def test_no_alive_locations(self):
        assert self.NUM_NODES == 1

        locations = list(get_locations())
        for location in locations:
            open(os.path.join(location, "disabled"), "w").close()

        wait(lambda: no_alive_location_alert(ls("//sys/exec_nodes")[0]))

    @authors("epsilond1", "pogorelov")
    def test_one_location_alive(self):
        assert self.NUM_NODES == 1

        locations = list(get_locations())
        assert len(locations) == 3
        for location in locations[:2]:
            open(os.path.join(location, "disabled"), "w").close()

        with Restarter(self.Env, NODES_SERVICE):
            pass

        alive_location = locations[2]

        job_count = 3

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=job_count)
        job_ids = wait_breakpoint(job_count=job_count)
        assert len(job_ids) == job_count

        for job_id in job_ids:
            verify_job_proxy_logs_exist([alive_location], job_id)

        release_breakpoint()
        op.track()

    @authors("epsilond1", "pogorelov")
    def test_slot_is_disabled(self):
        locations = list(get_locations())
        for location in locations:
            open(os.path.join(location, "disabled"), "w").close()

        with Restarter(self.Env, NODES_SERVICE):
            pass

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=1, track=False)

        with pytest.raises(Exception):
            wait_breakpoint(timeout=datetime.timedelta(seconds=10))

        op.abort()

    @authors("epsilond1", "pogorelov")
    def test_rising_location(self):
        locations = list(get_locations())
        assert len(locations) > 0

        open(os.path.join(locations[0], "disabled"), "w").close()

        with Restarter(self.Env, NODES_SERVICE):
            pass

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        job_id = wait_breakpoint()[0]
        release_breakpoint(job_id=job_id)
        op.track()

        verify_job_proxy_logs_exist(locations[1:], job_id)

        os.remove(os.path.join(locations[0], "disabled"))

        node_address = ls("//sys/exec_nodes")[0]

        def location_alert_cleared():
            alerts = get(f"//sys/exec_nodes/{node_address}/@alerts")
            return not any(
                alert.get("message") == "This location disabled"
                for alert in alerts
            )

        wait(location_alert_cleared)

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        job_id = wait_breakpoint()[0]
        release_breakpoint()
        op.track()

        verify_job_proxy_logs_exist(locations, job_id)

    @authors("epsilond1", "pogorelov")
    def test_location_alert(self):
        locations = list(get_locations())
        assert len(locations) > 0
        location = locations[0]

        update_nodes_dynamic_config({
            "exec_node": {
                "job_proxy_log_manager": {
                    "location_check_period": "30s",
                },
            },
        })

        time.sleep(1)  # Sleep for previous check period (1 second)

        open(os.path.join(location, "disabled"), "w").close()

        assert self.NUM_NODES == 1
        node_address = ls("//sys/exec_nodes")[0]

        def has_disabled_locations():
            alerts = get(f"//sys/exec_nodes/{node_address}/@alerts")
            return any(
                alert.get("message") == "Location disabled"
                for alert in alerts
            )

        with pytest.raises(WaitFailed, match="Wait failed"):
            wait(has_disabled_locations, timeout=5)
