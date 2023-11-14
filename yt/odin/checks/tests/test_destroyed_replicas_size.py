from yt_odin.logserver import FULLY_AVAILABLE_STATE, UNAVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_and_run_checks, wait


CHECK_OPTIONS = {
    "node_threshold": 1,
    "total_threshold": 1,
}


def test_destroyed_replicas_size(yt_env):
    yt_client = yt_env.yt_client

    proxy_url = yt_client.config["proxy"]["url"]
    checks_path = make_check_dir("destroyed_replicas_size", CHECK_OPTIONS)
    storage = configure_and_run_checks(proxy_url, checks_path)
    assert abs(storage.get_service_states("destroyed_replicas_size")[-1] - FULLY_AVAILABLE_STATE) <= 0.001

    yt_client.create("table", "//tmp/t")

    yt_client.write_table("<append=true>//tmp/t", [{"1": "a"}])
    yt_client.write_table("<append=true>//tmp/t", [{"1": "a"}])
    yt_client.write_table("<append=true>//tmp/t", [{"1": "a"}])

    for node in yt_client.get("//sys/cluster_nodes"):
        yt_client.set("//sys/cluster_nodes/{}/@resource_limits_overrides/removal_slots".format(node), 0)

    yt_client.remove("//tmp/t")

    def is_unavailable():
        storage = configure_and_run_checks(proxy_url, checks_path)
        return abs(storage.get_service_states("destroyed_replicas_size")[-1] - UNAVAILABLE_STATE) <= 0.001

    wait(is_unavailable)
