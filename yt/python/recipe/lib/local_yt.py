import copy
import os

import yatest.common
from deepmerge import always_merger
from mapreduce.yt.python import yt_stuff

DEFAULT_CONFIG = {
    "enable_debug_logging": True,
    "wait_tablet_cell_initialization": True,
    "node_count": 2,
    "node_config": {
        "tablet_node": {
            "resource_limits": {
                "tablet_dynamic_memory": 2147483648,
                "tablet_static_memory": 8589934592,
            }
        }
    },
}


def merge(base, patch):
    return always_merger.merge(copy.deepcopy(base), patch)


def start(replicas, groups):
    """start a local yt clusters"""

    clusters = {}
    for i, name in enumerate(sorted(replicas, reverse=True)):
        work_dir = yatest.common.output_path("yt_wd_" + name)
        os.makedirs(work_dir)
        config = merge(DEFAULT_CONFIG, replicas[name])
        yt_config = yt_stuff.YtConfig(yt_id=name, cell_tag=i, yt_work_dir=work_dir, **config)

        cluster = yt_stuff.YtStuff(config=yt_config)
        cluster.start_local_yt()
        clusters[name] = cluster

    # update clusters config to union all replicas in single cluster
    cluster_config = {cluster.yt_id: cluster.get_cluster_config() for cluster in clusters.values()}
    for cluster in clusters.values():
        client = cluster.get_yt_client()
        client.set("//sys/clusters", cluster_config)
        client.config["read_retries"]["enable"] = True
        client.config["write_retries"]["enable"] = True

    # add common acl groups to all clusters if needed
    if groups:
        for cluster in clusters.values():
            client = cluster.get_yt_client()
            for group in groups:
                client.create("group", attributes={"name": group})

    return clusters
