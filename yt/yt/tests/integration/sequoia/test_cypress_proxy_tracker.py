from yt_env_setup import YTEnvSetup, Restarter, CYPRESS_PROXIES_SERVICE

from yt_commands import (
    authors,
    wait, build_snapshot, build_master_snapshots,
    get, ls, remove, exists, set,
)

from datetime import datetime, timezone
from time import sleep


##################################################################


class TestCypressProxyTracker(YTEnvSetup):
    ENABLE_MULTIDAEMON = False
    NUM_MASTERS = 3
    NUM_CYPRESS_PROXIES = 2

    DELTA_CYPRESS_PROXY_CONFIG = {
        "heartbeat_period": 1000,
    }

    @authors("kvk1920")
    def test_recovery(self):
        primary_master_config = self.Env.configs["master"][0]["primary_master"]

        build_snapshot(cell_id=primary_master_config["cell_id"], set_read_only=False)

        for p in ls("//sys/cypress_proxies"):
            remove(f"//sys/cypress_proxies/{p}")

        wait(lambda: len(ls("//sys/cypress_proxies")) == 2)

        self.Env.kill_service("master", indexes=[1])
        sleep(3)

        self.Env.start_master_cell(set_config=False)
        address = primary_master_config["addresses"][1]
        wait(lambda: get(
            f"//sys/primary_masters/{address}/orchid/monitoring/hydra/active",
            default=False), ignore_exceptions=True)
        sleep(3)

        assert len(ls("//sys/cypress_proxies")) == 2

    @authors("kvk1920")
    def test_cypress_proxy_registry(self):
        assert get("//sys/cypress_proxies/@count") == self.NUM_CYPRESS_PROXIES

        cypress_proxy = ls("//sys/cypress_proxies")[0]
        cypress_proxy_object_id = get(f"//sys/cypress_proxies/{cypress_proxy}/@id")
        cypress_proxy_reign = get(f"//sys/cypress_proxies/{cypress_proxy}/orchid/sequoia_reign")

        master = ls("//sys/primary_masters")[0]
        master_reign = get(f"//sys/primary_masters/{master}/orchid/sequoia_reign")

        assert cypress_proxy_reign == master_reign

        remove(f"//sys/cypress_proxies/{cypress_proxy}")
        wait(lambda: exists(f"//sys/cypress_proxies/{cypress_proxy}"))
        assert cypress_proxy_object_id != get(f"//sys/cypress_proxies/{cypress_proxy}/@id")

    @authors("kvk1920")
    def test_cypress_proxy_version(self):
        version = ls("//sys/cypress_proxies", attributes=["version"])[0].attributes["version"]
        assert ".".join(map(str, self.Env.get_component_version("ytserver-cypress-proxy").abi)) in version

    @authors("kvk1920")
    def test_cypress_proxy_persistent_heartbeat_period(self):
        set("//sys/@config/cypress_proxy_tracker/persistent_heartbeat_period", 1000)

        now = datetime.now(timezone.utc)

        def check():
            last_persistent_heartbeat_time = ls("//sys/cypress_proxies", attributes=["last_persistent_heartbeat_time"])[0].attributes["last_persistent_heartbeat_time"]
            last_persistent_heartbeat_time = datetime.strptime(last_persistent_heartbeat_time, "%Y-%m-%dT%H:%M:%S.%fZ")
            last_persistent_heartbeat_time = last_persistent_heartbeat_time.replace(tzinfo=timezone.utc)
            return last_persistent_heartbeat_time > now

        wait(check)

    @authors("kvk1920")
    def test_read_only_registration(self):
        set("//sys/@config/cypress_proxy_tracker/persistent_heartbeat_period", 200)
        build_master_snapshots(set_read_only=True)
        with Restarter(self.Env, CYPRESS_PROXIES_SERVICE):
            pass
        sleep(1)
        # Should not fail.
        get("//sys/@config")
