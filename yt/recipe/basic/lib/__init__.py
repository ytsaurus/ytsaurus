from yt import wrapper as yt

import yatest.common

import json
import os

from multiprocessing.pool import ThreadPool

__all__ = ["get_yt_clusters", "get_yt_instances"]

_YT_CLUSTER_INFO_FILE = "yt_cluster_info.json"
_RECIPE_INFO_FILE = "yt_recipe_info.json"


class _YtCluster(object):
    def __init__(self, cfg):
        self.yt_id = cfg["id"]
        self.proxy_address = cfg["proxy_address"]
        self.client = yt.YtClient(proxy=self.proxy_address)

    def get_uri(self):
        return "%s#%s" % (self.yt_id, self.proxy_address)

    def get_yt_client(self):
        return self.client

    def get_proxy_address(self):
        return self.proxy_address


class _YtClusterGroup(object):
    """Contains information about whole yt cluster group.

    Allows to get cluster info by sequence number and by id.
    cluster_group[0] - first cluster in group
    cluster_group["primary"] - cluster with id "primary"
    cluster_group.primary  - alternative variant to get primary cluster
    """

    def __init__(self, config):
        clusters = {}
        for i, x in enumerate(config):
            cluster = _YtCluster(x)
            clusters[i] = cluster
            clusters[cluster.yt_id] = cluster
        self._clusters = clusters
        self._count = len(config)

    @property
    def primary_cluster(self):
        """Returns primary cluster."""
        return self._clusters[0]

    @property
    def replica_clusters(self):
        """Returns list of secondary clusters."""
        return [self._clusters[i] for i in range(1, self._count)]

    def __len__(self):
        return self._count

    def __getitem__(self, item):
        return self._clusters[item]

    def __getattr__(self, item):
        return self._clusters[item]

    def __iter__(self):
        for i in range(self._count):
            yield self._clusters[i]


class _YtLocalInstance(object):
    def __init__(self, cfg):
        self.yt_id = cfg["yt_id"]
        self.work_dir = cfg["yt_work_dir"]
        self.local_exec = cfg["yt_local_exec"]

    def stop(self):
        yatest.common.execute(
            self.local_exec + ["stop", os.path.join(self.work_dir, self.yt_id)]
        )


class _YtLocalInstanceGroup(object):
    """Contains information about whole yt cluster group local instances.

    Allows to get cluster info by sequence number and by id.
    instance_group[0] - first cluster in group
    instance_group["primary"] - cluster with id "primary"
    instance_group.primary  - alternative variant to get primary cluster
    """
    def __init__(self, config):
        local_instances = {}
        for i, x in enumerate(config):
            local_instance = _YtLocalInstance(x)
            local_instances[i] = local_instance
            local_instances[local_instance.yt_id] = local_instance
        self._local_instances = local_instances
        self._count = len(config)

    @property
    def primary_cluster(self):
        """Returns primary cluster."""
        return self._local_instances[0]

    @property
    def replica_clusters(self):
        """Returns list of secondary clusters."""
        return [self._local_instances[i] for i in range(1, self._count)]

    def __len__(self):
        return self._count

    def __getitem__(self, item):
        return self._local_instances[item]

    def __getattr__(self, item):
        return self._local_instances[item]


def get_yt_clusters():
    with open(yatest.common.output_path(_YT_CLUSTER_INFO_FILE), "r") as fd:
        return _YtClusterGroup(json.load(fd))


def dump_yt_clusters(clusters):
    """Saves yt clusters information in json file (for internal use only)."""
    cluster_list = []
    for cluster in clusters:
        cluster_list.append({"id": cluster.yt_id, "proxy_address": "localhost:%d" % cluster.yt_proxy_port})

    with open(yatest.common.output_path(_YT_CLUSTER_INFO_FILE), "w") as fd:
        json.dump(cluster_list, fd)


def get_yt_instances():
    recipe_file = yatest.common.output_path(_RECIPE_INFO_FILE)

    if not os.path.exists(recipe_file):
        return _YtLocalInstanceGroup(dict())

    with open(recipe_file, "r") as fd:
        return _YtLocalInstanceGroup(json.load(fd))


def dump_yt_instances(clusters):
    recipe_info = [
        {"yt_id": x.yt_id, "yt_work_dir": x.yt_work_dir, "yt_local_exec": x.yt_local_exec}
        for x in clusters.values()
    ]

    with open(yatest.common.output_path(_RECIPE_INFO_FILE), "w") as fd:
        json.dump(recipe_info, fd)


def run_concurrent(func, args):
    pool = ThreadPool(len(args))
    pool.map(func, args)
    pool.close()
    pool.join()
