# coding=utf-8
from yt_odin.logserver import FULLY_AVAILABLE_STATE, UNAVAILABLE_STATE, PARTIALLY_AVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_and_run_checks, wait

from yt.wrapper import YtClient
from yt.common import update, update_inplace
from yt import yson

import pytest
import time

QUEUE_AGENT_DYNAMIC_CONFIG_PATH = "//sys/queue_agents/config"
CHECK_OPTIONS = {
    "max_lag_ms": 3_000,  # 3 seconds
    "ignore_unavailable_instances": False,  # All instances should be available in tests
}


def test_queue_agent_controller_liveness(yt_env_one_queue_agent):
    client = yt_env_one_queue_agent.yt_client
    proxy_url = client.config["proxy"]["url"]

    client.create("queue_consumer", "//tmp/consumer")
    # Wait for the second pass
    time.sleep(10)

    checks_path = make_check_dir("queue_agent_controller_liveness", CHECK_OPTIONS)
    storage = configure_and_run_checks(proxy_url, checks_path)
    status = storage.get_service_states("queue_agent_controller_liveness")[-1]
    assert status == FULLY_AVAILABLE_STATE


def test_queue_agent_controller_liveness_partially_available(yt_env_one_queue_agent):
    client = yt_env_one_queue_agent.yt_client
    proxy_url = client.config["proxy"]["url"]
    checks_path = make_check_dir("queue_agent_controller_liveness", CHECK_OPTIONS)
    storage = configure_and_run_checks(proxy_url, checks_path)
    status = storage.get_service_states("queue_agent_controller_liveness")[-1]
    assert status == PARTIALLY_AVAILABLE_STATE


@pytest.mark.parametrize("object_type", ["queue", "consumer"])
def test_queue_agent_controller_liveness_unavailable(yt_env_one_queue_agent, object_type):
    client: YtClient = yt_env_one_queue_agent.yt_client
    proxy_url = client.config["proxy"]["url"]

    bad_object_path = f"//tmp/{object_type}_bad"

    if object_type == "queue":
        client.create("table", bad_object_path, attributes={
            "dynamic": True,
            "schema": [{"name": "data", "type": "string"}],
        })
        client.mount_table(bad_object_path, sync=True)
    else:
        client.create("queue_consumer", bad_object_path)

    cluster_name = client.get("//sys/@cluster_name")
    instance = str(client.list("//sys/queue_agents/instances")[0])

    # We need to wait for the second pass of #bad_object or otherwise it won't be counted
    time.sleep(10)

    config = client.get(QUEUE_AGENT_DYNAMIC_CONFIG_PATH)
    update_inplace(config, {
        "queue_agent": {
            "controller": {
                "delayed_objects": [yson.loads(f"<cluster={cluster_name}>\"{bad_object_path}\"".encode())],
                "controller_delay_duration": 3_600_000,  # 1 hour
            },
        }
    })
    client.set(QUEUE_AGENT_DYNAMIC_CONFIG_PATH, config)

    def check_config_updated():
        effective_config = client.get(f"//sys/queue_agents/instances/{instance}/orchid/dynamic_config_manager/effective_config")
        return update(effective_config, config) == effective_config

    wait(check_config_updated)

    # Wait for a new pass to start and lag to grow
    time.sleep(10)

    checks_path = make_check_dir("queue_agent_controller_liveness", CHECK_OPTIONS)
    storage = configure_and_run_checks(proxy_url, checks_path)
    status = storage.get_service_states("queue_agent_controller_liveness")[-1]
    assert status == UNAVAILABLE_STATE
