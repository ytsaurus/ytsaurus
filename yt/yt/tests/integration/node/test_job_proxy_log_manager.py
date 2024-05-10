from yt_env_setup import YTEnvSetup

from yt_commands import (
    run_test_vanilla, with_breakpoint, wait_breakpoint, authors, wait)

import os.path
import time


class TestJobProxyLogManager(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
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
            "job_proxy_log_manager": {
                "logs_storage_period": "1s",
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

    @authors("tagirhamitov")
    def test_removing_logs(self):
        job_count = 3

        op = run_test_vanilla("", job_count=job_count)

        wait(lambda: len(op.list_jobs()) == job_count)
        job_ids = op.list_jobs()
        assert len(job_ids) == job_count
        op.get_job_count

        time.sleep(1.5)

        for job_id in job_ids:
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
            assert not os.path.exists(log_path)
