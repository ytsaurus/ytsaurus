import os
from typing import Any

from yt import wrapper as yt

from .proxy import YtClientProxy


class YtClientFactory:
    def __init__(self, dry_run: bool, token: str | None, per_cluster_config: dict[str, Any] | None = None):
        self._dry_run: bool = dry_run
        self._token: str | None = token
        self._aliases: dict[str, str] = {}
        self._per_cluster_config: dict[str, Any] = per_cluster_config or dict()

    def is_dry_run(self) -> bool:
        return self._dry_run

    def add_alias(self, name: str, address: str):
        self._aliases[name] = address

    def get_proxy_address(self, cluster: str) -> str | None:
        name, address = self.split_name(cluster)
        address = address or self._aliases.get(name, name)
        return address

    def __call__(self, cluster: str, config: dict | None = None) -> YtClientProxy:
        name, address = self.split_name(cluster)
        address = address or self._aliases.get(name, name)
        return YtClientProxy(
            self._dry_run, name, yt.YtClient(address, token=self._token, config=self._get_config(cluster, config))
        )

    def _get_config(self, cluster: str, patch: dict | None = None) -> dict:
        config = yt.default_config.get_config_from_env()
        config["tablets_ready_timeout"] = 300 * 1000
        config["backend"] = "rpc"

        cluster_config = self._per_cluster_config.get(cluster, dict())
        config.update(cluster_config)
        if patch:
            config.update(patch)
        return config

    @staticmethod
    def split_name(cluster_name: str) -> tuple[str, str | None]:
        name, _, address = cluster_name.partition("#")
        return (name, address)


def get_yt_client_factory(
    dry_run: bool, token: str | None = None, per_cluster_config: dict[str, Any] | None = None
) -> YtClientFactory:
    """Return functor to create yt clients."""
    token = token or os.getenv("YT_TOKEN")
    if not token:
        try:
            with open(os.path.expanduser("~/.yt/token")) as fd:
                token = fd.read().strip()
        except OSError:
            pass
    assert token, "Please specify token for YT via YT_TOKEN env variable."

    return YtClientFactory(dry_run, token, per_cluster_config)
