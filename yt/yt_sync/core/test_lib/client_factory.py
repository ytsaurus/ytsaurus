from typing import Any

from yt.yt_sync.core import YtClientFactory, get_yt_client_factory


def get_test_yt_client_factory(yt_cluster: Any, dry_run: bool) -> YtClientFactory:
    factory = get_yt_client_factory(dry_run, token="test")
    for i in range(len(yt_cluster)):
        yt_instance = yt_cluster[i]
        factory.add_alias(yt_instance.yt_id, yt_instance.proxy_address)
    return factory
