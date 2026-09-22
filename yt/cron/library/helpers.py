from yt.wrapper.client import YtClient

import os
import simplejson as json

try:
    from .helpers_yandex import is_date_holiday  # noqa
except ImportError:
    pass


def create_yt_client(cluster_name, retry_count=None):
    config = dict()

    if "YT_TOKEN_PATH" in os.environ:
        config["token_path"] = os.environ["YT_TOKEN_PATH"]
    if retry_count is not None:
        config["proxy"] = dict(retries=dict(count=retry_count))

    return YtClient(proxy=cluster_name, config=config)


def get_all_clusters_with_config(clusters_config_proxy, clusters_config_path, skipped_clusters=None, specified_clusters=None):
    client = YtClient(config={
        "proxy": {
            "url": clusters_config_proxy
        },
        "token_path": os.environ["YT_TOKEN_PATH"]
    })
    clusters_configuration = client.get(clusters_config_path)

    whitelist = set(json.loads(os.environ.get("YT_CRON_WHITELIST"))) if "YT_CRON_WHITELIST" in os.environ else None
    blacklist = set(json.loads(os.environ.get("YT_CRON_BLACKLIST", "[]")) +
                    json.loads(os.environ.get("BANNED_CLUSTERS", "[]")))

    def is_valid_cluster(cluster, cluster_type):
        if whitelist is not None and cluster not in whitelist:
            return False
        if cluster in blacklist:
            return False
        if skipped_clusters is not None and cluster in skipped_clusters:
            return False
        if specified_clusters is not None and cluster not in specified_clusters:
            return False
        if cluster_type == "closing":
            return False
        return True

    def _filter_config(cluster_config):
        allowed = {
            "type",
            "proxy",
            "enable_tls"
        }
        return {
            k: v for k, v in cluster_config.items() if k in allowed
        }

    clusters_with_config = {}
    for cluster_name, cluster_config in clusters_configuration.items():
        cluster_type = cluster_config["type"]
        if is_valid_cluster(cluster_name, cluster_type):
            clusters_with_config[cluster_name] = _filter_config(cluster_config)

    return clusters_with_config
