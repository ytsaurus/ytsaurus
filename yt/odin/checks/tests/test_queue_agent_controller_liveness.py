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
    "ignore_unreachable_instances": False,  # All instances should be reachable in tests
}


def wait_for_controller_passes(client, instance, object_path, pass_count=2):
    """Waits until the controller of #object_path has performed at least #pass_count passes.

    An object is only reported by the controller once it has been passed over, so waiting for
    this explicitly is the only reliable way to tell that a subsequent controller delay will
    actually freeze an already known pass instant instead of preventing the very first pass.
    """
    pass_instants = set()

    def has_enough_passes():
        inactive_objects = client.get(
            f"//sys/queue_agents/instances/{instance}/orchid/queue_agent/controller_info/inactive_objects")
        for controller_passes in inactive_objects.values():
            for controller_pass in controller_passes:
                if str(controller_pass["path"]) == object_path:
                    pass_instants.add(controller_pass["pass_instant"])
        return len(pass_instants) >= pass_count

    # The orchid node is not there until the queue agent starts leading, so transient errors are expected.
    wait(has_enough_passes, ignore_exceptions=True)


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
    wait_for_controller_passes(client, instance, bad_object_path)

    config = client.get(QUEUE_AGENT_DYNAMIC_CONFIG_PATH)
    update_inplace(config, {
        "queue_agent": {
            "controller": {
                "delayed_objects": [yson.loads(f"<cluster={cluster_name}>\"{bad_object_path}\"".encode())],
                "controller_delay": 3_600_000,  # 1 hour
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
