from typing import Any

from yt.yt_sync.core.client import get_yt_client_factory
from yt.yt_sync.core.client import YtClientFactory


def get_test_yt_client_factory(yt_cluster: Any, dry_run: bool) -> YtClientFactory:
    factory = get_yt_client_factory(dry_run, token="test")
    for i in range(len(yt_cluster)):
        yt_instance = yt_cluster[i]
        factory.add_alias(yt_instance.yt_id, yt_instance.proxy_address)
    return factory
