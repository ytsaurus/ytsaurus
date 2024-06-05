from yt_env_setup import (YTEnvSetup, Restarter, NODES_SERVICE)

from yt_commands import (
    run_test_vanilla, with_breakpoint, wait_breakpoint, authors, release_breakpoint,
    update_nodes_dynamic_config, wait, update)

import os.path

BASE_NODE_CONFIG = {
    "exec_node": {
        "job_controller": {
            "resource_limits": {
                "user_slots": 5,
                "cpu": 5,
                "memory": 5 * 1024 ** 3,
            }
        },
        "job_proxy": {
            "job_proxy_logging": {
                "mode": "per_job_directory",
            },
        },
    },
    "job_resource_manager": {
        "resource_limits": {
            "user_slots": 5,
            "cpu": 5,
            "memory": 5 * 1024 ** 3,
        }
    },
}


class TestJobProxyLogManagerBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    def job_proxy_log_exists(self, job_id):
        sharding_key = job_id.split("-")[0]
        if len(sharding_key) < 8:
            sharding_key = "0"
        else:
            sharding_key = sharding_key[0]
        log_path = os.path.join(
            self.path_to_run,
            "logs/job_proxy-0",
            sharding_key,
            job_id,
            "job_proxy.log"
        )
        return os.path.exists(log_path)


class TestJobProxyLogManager(TestJobProxyLogManagerBase):
    DELTA_NODE_CONFIG = update(
        BASE_NODE_CONFIG,
        {
            "exec_node": {
                "job_proxy_log_manager": {
                    "logs_storage_period": "1s",
                },
            },
        }
    )

    @authors("tagirhamitov")
    def test_removing_logs(self):
        job_count = 3

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=job_count)
        job_ids = wait_breakpoint(job_count=job_count)
        assert len(job_ids) == job_count
        release_breakpoint()

        op.track()

        for job_id in job_ids:
            assert self.job_proxy_log_exists(job_id)

        for job_id in job_ids:
            wait(lambda: not self.job_proxy_log_exists(job_id))

    @authors("tagirhamitov")
    def test_removing_logs_on_start(self):
        job_count = 3

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=job_count)
        job_ids = wait_breakpoint(job_count=job_count)
        assert len(job_ids) == job_count
        release_breakpoint()

        op.track()

        for job_id in job_ids:
            assert self.job_proxy_log_exists(job_id)

        with Restarter(self.Env, NODES_SERVICE):
            pass

        for job_id in job_ids:
            wait(lambda: not self.job_proxy_log_exists(job_id))


class TestJobProxyLogManagerDynamicConfig(TestJobProxyLogManagerBase):
    DELTA_NODE_CONFIG = BASE_NODE_CONFIG

    @authors("tagirhamitov")
    def test_dynamic_config(self):
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        job_id_1 = wait_breakpoint()[0]
        release_breakpoint(job_id=job_id_1)
        op.track()

        assert self.job_proxy_log_exists(job_id_1)

        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_proxy_log_manager": {
                        "logs_storage_period": "1s",
                    },
                }
            }
        })

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        job_id_2 = wait_breakpoint()[0]
        release_breakpoint(job_id=job_id_2)
        op.track()

        assert self.job_proxy_log_exists(job_id_2)

        wait(lambda: not self.job_proxy_log_exists(job_id_2))
        assert self.job_proxy_log_exists(job_id_1)
