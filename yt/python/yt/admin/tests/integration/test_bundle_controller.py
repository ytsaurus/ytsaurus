from yt.admin.bundle_controller import (
    ZONE_PATH,
    guess_default_config,
    run_bundle_controller_create_bundle,
    run_bundle_controller_set_resource_limits,
)
import yt.wrapper

from yt_commands import authors, create, create_account, exists, get, remove, remove_tablet_cell_bundle, set, wait
from yt_env_setup import YTEnvSetup

import pytest

from typing import Generator


CPU = 8
MEMORY = 8 * 2**30
SYSTEM_QUOTAS_ACCOUNT = "bundle_system_quotas"


class TestBundleController(YTEnvSetup):
    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1
    NUM_MASTERS = 1
    NUM_NODES = 0

    @pytest.fixture
    def yt_client(self) -> Generator[yt.wrapper.YtClient, None, None]:
        yield self.Env.create_client()

    def _ensure_system_quotas_account(self) -> None:
        if not exists(f"//sys/accounts/{SYSTEM_QUOTAS_ACCOUNT}"):
            create_account(SYSTEM_QUOTAS_ACCOUNT)

    def _setup_zone(self) -> None:
        node_resources = {"vcpu": CPU * 1000, "memory": MEMORY, "net_bytes": 0}
        cpu_limits, memory_limits = guess_default_config(CPU, MEMORY)

        create("map_node", ZONE_PATH, recursive=True, ignore_existing=True)
        set(ZONE_PATH + "/@tablet_node_sizes", {
            "regular": {
                "default_config": {
                    "cpu_limits": cpu_limits._asdict(),
                    "memory_limits": memory_limits._asdict(),
                },
                "resource_guarantee": node_resources,
            },
        })
        set(ZONE_PATH + "/@rpc_proxy_sizes", {
            "regular": {
                "resource_guarantee": node_resources,
            },
        })

    @staticmethod
    def _remove_and_wait(path: str) -> None:
        if not exists(path):
            return
        remove(path)
        wait(lambda: not exists(path))

    def teardown_method(self, method):
        if exists("//sys/tablet_cell_bundles/test_bundle"):
            remove_tablet_cell_bundle("test_bundle")
            wait(lambda: not exists("//sys/tablet_cell_bundles/test_bundle"))
        self._remove_and_wait(f"//sys/accounts/test_bundle_{SYSTEM_QUOTAS_ACCOUNT}")
        self._remove_and_wait(f"//sys/accounts/{SYSTEM_QUOTAS_ACCOUNT}")
        if exists("//sys/bundle_controller"):
            remove("//sys/bundle_controller", recursive=True)
        super(TestBundleController, self).teardown_method(method)

    @authors("ilyaibraev")
    def test_set_bundle_resource_limits(self, yt_client):
        self._setup_zone()
        node_count = 2

        run_bundle_controller_set_resource_limits(
            bundle_name="default",
            node_count=node_count,
            dry_run=False,
            yes=True,
            client=yt_client,
        )

        assert get("//sys/tablet_cell_bundles/default/@resource_limits/cpu") == node_count * CPU
        assert get("//sys/tablet_cell_bundles/default/@resource_limits/memory") == node_count * MEMORY

    @authors("ilyaibraev")
    def test_set_bundle_resource_limits_dry_run(self, yt_client):
        self._setup_zone()
        before = get("//sys/tablet_cell_bundles/default/@resource_limits/cpu")

        run_bundle_controller_set_resource_limits(
            bundle_name="default",
            node_count=5,
            dry_run=True,
            yes=True,
            client=yt_client,
        )

        assert get("//sys/tablet_cell_bundles/default/@resource_limits/cpu") == before

    @authors("ilyaibraev")
    def test_create_bundle(self, yt_client):
        self._ensure_system_quotas_account()
        self._setup_zone()

        run_bundle_controller_create_bundle(
            bundle_name="test_bundle",
            dry_run=False,
            yes=True,
            client=yt_client,
        )

        assert exists("//sys/tablet_cell_bundles/test_bundle")
        assert exists(f"//sys/accounts/test_bundle_{SYSTEM_QUOTAS_ACCOUNT}")

        options = get("//sys/tablet_cell_bundles/test_bundle/@options")
        assert options["changelog_account"] == f"test_bundle_{SYSTEM_QUOTAS_ACCOUNT}"
        assert options["snapshot_account"] == f"test_bundle_{SYSTEM_QUOTAS_ACCOUNT}"

        assert get("//sys/tablet_cell_bundles/test_bundle/@zone") == "zone_default"
        assert get("//sys/tablet_cell_bundles/test_bundle/@enable_bundle_controller")

    @authors("ilyaibraev")
    def test_create_bundle_is_idempotent(self, yt_client):
        self._ensure_system_quotas_account()
        self._setup_zone()

        for _ in range(2):
            run_bundle_controller_create_bundle(
                bundle_name="test_bundle",
                dry_run=False,
                yes=True,
                client=yt_client,
            )

        assert exists("//sys/tablet_cell_bundles/test_bundle")
