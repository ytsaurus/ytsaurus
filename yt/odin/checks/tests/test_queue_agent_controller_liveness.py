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


def create_object(client, object_type, path):
    if object_type == "queue":
        client.create("table", path, attributes={
            "dynamic": True,
            "schema": [{"name": "data", "type": "string"}],
        })
        client.mount_table(path, sync=True)
    else:
        client.create("queue_consumer", path)


def update_queue_agent_config(client, instance, patch):
    config = client.get(QUEUE_AGENT_DYNAMIC_CONFIG_PATH)
    update_inplace(config, patch)
    client.set(QUEUE_AGENT_DYNAMIC_CONFIG_PATH, config)

    def check_config_updated():
        effective_config = client.get(f"//sys/queue_agents/instances/{instance}/orchid/dynamic_config_manager/effective_config")
        return update(effective_config, config) == effective_config

    wait(check_config_updated)


def delay_object(client, instance, object_cluster, object_path):
    update_queue_agent_config(client, instance, {
        "queue_agent": {
            "controller": {
                "delayed_objects": [yson.loads(f"<cluster={object_cluster}>\"{object_path}\"".encode())],
                "controller_delay": 3_600_000,  # 1 hour
            },
        }
    })


@pytest.mark.parametrize("use_stage_clusters", [False, True])
@pytest.mark.parametrize("object_type", ["queue", "consumer"])
def test_queue_agent_controller_liveness_unavailable(yt_env_one_queue_agent, object_type, use_stage_clusters):
    client: YtClient = yt_env_one_queue_agent.yt_client
    proxy_url = client.config["proxy"]["url"]

    bad_object_path = f"//tmp/{object_type}_bad"
    create_object(client, object_type, bad_object_path)

    cluster_name = str(client.get("//sys/@cluster_name"))
    instance = str(client.list("//sys/queue_agents/instances")[0])

    # We need to wait for the second pass of #bad_object or otherwise it won't be counted
    wait_for_controller_passes(client, instance, bad_object_path)

    delay_object(client, instance, cluster_name, bad_object_path)

    # Wait for a new pass to start and lag to grow
    time.sleep(10)

    options = CHECK_OPTIONS
    if use_stage_clusters:
        options = update(CHECK_OPTIONS, {"queue_agent_stage_clusters": {cluster_name: proxy_url}})
    checks_path = make_check_dir("queue_agent_controller_liveness", options)
    storage = configure_and_run_checks(proxy_url, checks_path, cluster_name)
    status = storage.get_service_states("queue_agent_controller_liveness")[-1]
    assert status == UNAVAILABLE_STATE


def test_queue_agent_controller_liveness_unreachable_foreign_stage(yt_env_one_queue_agent):
    client = yt_env_one_queue_agent.yt_client
    proxy_url = client.config["proxy"]["url"]

    options = update(CHECK_OPTIONS, {"queue_agent_stage_clusters": {"foreign": "localhost:1"}})
    checks_path = make_check_dir("queue_agent_controller_liveness", options)
    storage = configure_and_run_checks(proxy_url, checks_path)
    record = storage.get_all_records("queue_agent_controller_liveness")[-1]
    assert record.state == FULLY_AVAILABLE_STATE
    assert "Could not list queue agent instances on cluster foreign" in record.messages


@pytest.mark.parametrize("object_type", ["queue", "consumer"])
def test_queue_agent_controller_liveness_unavailable_on_remote_cluster(yt_env_two_clusters_one_queue_agent, object_type):
    queue_agent_environment, remote_environment = yt_env_two_clusters_one_queue_agent
    queue_agent_client: YtClient = queue_agent_environment.yt_client
    remote_client: YtClient = remote_environment.yt_client
    queue_agent_proxy_url = queue_agent_client.config["proxy"]["url"]
    remote_proxy_url = remote_client.config["proxy"]["url"]
    queue_agent_cluster_name = str(queue_agent_client.get("//sys/@cluster_name"))
    remote_cluster_name = str(remote_client.get("//sys/@cluster_name"))

    instance = str(queue_agent_client.list("//sys/queue_agents/instances")[0])
    update_queue_agent_config(queue_agent_client, instance, {
        "cypress_synchronizer": {
            "clusters": [queue_agent_cluster_name, remote_cluster_name],
        },
    })

    bad_object_path = f"//tmp/{object_type}_bad"
    create_object(remote_client, object_type, bad_object_path)

    # We need to wait for the second pass of #bad_object or otherwise it won't be counted
    wait_for_controller_passes(queue_agent_client, instance, bad_object_path)

    delay_object(queue_agent_client, instance, remote_cluster_name, bad_object_path)

    # Wait for a new pass to start and lag to grow
    time.sleep(10)

    options = update(CHECK_OPTIONS, {"queue_agent_stage_clusters": {queue_agent_cluster_name: queue_agent_proxy_url}})
    checks_path = make_check_dir("queue_agent_controller_liveness", options)
    storage = configure_and_run_checks(remote_proxy_url, checks_path, remote_cluster_name)
    status = storage.get_service_states("queue_agent_controller_liveness")[-1]
    assert status == UNAVAILABLE_STATE
