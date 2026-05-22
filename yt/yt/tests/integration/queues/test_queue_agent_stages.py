from yt.common import YtResponseError

from yt_queue_agent_test_base import TestQueueAgentBase

from yt_commands import (authors, get, set, wait)

import pytest

##################################################################


@pytest.mark.enabled_multidaemon
class TestQueueAgentStages(TestQueueAgentBase):
    NUM_REMOTE_CLUSTERS = 1
    CLUSTERS = ["primary", "remote_0"]

    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "watching",
        },
    }

    ENABLE_MULTIDAEMON = True

    @authors("apachee")
    @pytest.mark.timeout(120)
    @pytest.mark.parametrize("cluster_with_stage", ["primary", "remote_0"])
    def test_stage_discovery(self, cluster_with_stage):
        self._create_queue("//tmp/q")

        assert get("//tmp/q/@queue_agent_stage") == "production"

        self._wait_for_component_passes()

        production_stage_config = get("//sys/@cluster_connection/queue_agent/stages/production")

        for cluster in self.CLUSTERS:
            set(f"//sys/clusters/{cluster}/queue_agent/stages", {})

        def check_not_found():
            try:
                get("//tmp/q/@queue_status/registrations")
            except YtResponseError as e:
                if "Queue agent stage \"production\" is not found" not in str(e):
                    return False

                return True

            return False

        wait(check_not_found, ignore_exceptions=True)

        set(f"//sys/clusters/{cluster_with_stage}/queue_agent/stages/production", production_stage_config)

        wait(lambda: get("//tmp/q/@queue_status/registrations") == [], ignore_exceptions=True)
