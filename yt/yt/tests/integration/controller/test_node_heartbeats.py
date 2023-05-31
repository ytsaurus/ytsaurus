from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, map, wait,
    write_table, update_controller_agent_config, update_nodes_dynamic_config)

##################################################################


class TestNodeHeartbeats(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "scheduler_connector": {"heartbeat_period": 100},
            "controller_agent_connector": {"heartbeat_period": 100},
        },
        "dynamic_config_manager": {
            "update_period": 100,
        },
    }

    def _test(self, fixup):
        update_controller_agent_config("full_job_info_wait_timeout", 1000)
        update_nodes_dynamic_config({
            "exec_agent": {
                "controller_agent_connector": {
                    "test_heartbeat_delay": 10000,
                }
            }
        })

        create("table", "//tmp/in")
        create("table", "//tmp/out")
        for _ in range(5):
            write_table("<append=true>//tmp/in", {"foo": "bar"})

        command = "cat;"
        op = map(
            track=False,
            in_="//tmp/in",
            out="//tmp/out",
            command=command,
            spec={"data_size_per_job": 1},
        )

        wait(lambda: op.get_job_count("aborted") >= 3, ignore_exceptions=True)
        assert op.get_state() == "running"

        fixup()

        def check_operation_completed():
            return op.get_state() == "completed"
        wait(check_operation_completed)

        op.track()

    @authors("pogorelov")
    def test_job_abort_on_heartbeat_timeout(self):
        def fixup():
            update_nodes_dynamic_config({
                "exec_agent": {
                    "controller_agent_connector": {
                        "test_heartbeat_delay": 0,
                    }
                }
            })

        self._test(fixup)
