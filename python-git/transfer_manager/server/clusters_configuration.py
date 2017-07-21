from yt.transfer_manager.server.helpers import filter_out_keys
from yt.transfer_manager.server.yt_client import YtClientWithNotifications

from yt.tools.hadoop import Airflow, Hdfs, Hive, HBase
from yt.tools.remote_copy_tools import Kiwi

from yt.wrapper.common import update

from yt.packages.six import iteritems

import yt.wrapper as yt

from copy import deepcopy

class ClustersConfiguration(object):
    def __init__(self, clusters, availability_graph, kiwi_transmitter, hadoop_transmitter):
        self.clusters = clusters
        self.availability_graph = availability_graph
        self.kiwi_transmitter = kiwi_transmitter
        self.hadoop_transmitter = hadoop_transmitter


def get_clusters_configuration_from_config(config):
    config = deepcopy(config)
    clusters = {}
    for name, cluster_description in iteritems(config["clusters"]):
        type = cluster_description["type"]
        options = cluster_description.get("options", {})

        if type == "yt":
            if "config" not in options:
                options["config"] = {}
            options["config"] = update(config["yt_client_config"], options["config"])
            clusters[name] = YtClientWithNotifications(**options)
            clusters[name]._pools = cluster_description.get("pools", {})
            if "version" in cluster_description:
                clusters[name]._version = cluster_description["version"]
        elif type == "kiwi":
            clusters[name] = Kiwi(**options)
        elif type == "hive":
            clusters[name] = Hive(**options)
        elif type == "hdfs":
            clusters[name] = Hdfs(**options)
        elif type == "hbase":
            clusters[name] = HBase(**options)
        else:
            raise yt.YtError("Incorrect cluster type " + options["type"])

        clusters[name]._name = name
        clusters[name]._type = type
        clusters[name]._parameters = filter_out_keys(cluster_description, ["type", "options"])

    availability_graph = config["availability_graph"]

    for name in availability_graph:
        edges = {}
        for neighbour in availability_graph[name]:
            if isinstance(neighbour, dict):
                edges[neighbour["name"]] = neighbour.get("options", {})
            else:
                edges[neighbour] = {}
        availability_graph[name] = edges

    for name in availability_graph:
        if name not in clusters:
            raise yt.YtError("Incorrect availability graph, cluster {} is missing".format(name))
        for neighbour in availability_graph[name]:
            if neighbour not in clusters:
                raise yt.YtError("Incorrect availability graph, cluster {} is missing".format(neighbour))

    kiwi_transmitter = None
    if "kiwi_transmitter" in config:
        name = config["kiwi_transmitter"]
        if name not in clusters:
            raise yt.YtError("Incorrect kiwi transmitter, cluster {} is missing".format(name))
        kiwi_transmitter = clusters[name]
        if kiwi_transmitter._type != "yt":
            raise yt.YtError("Kiwi transmitter must be YT cluster")

    hadoop_transmitter = None
    if "hadoop_transmitter" in config:
        hadoop_transmitter_config = config["hadoop_transmitter"]
        if hadoop_transmitter_config["type"] != "airflow":
            raise yt.YtError("Hadoop transmitter must be airflow client")
        hadoop_transmitter = Airflow(**hadoop_transmitter_config["options"])
        hadoop_transmitter._name = hadoop_transmitter_config["name"]

    return ClustersConfiguration(clusters, availability_graph, kiwi_transmitter, hadoop_transmitter)
