from .helpers import get_cluster_version

import yt.wrapper as yt

import os

def _has_write_permission_on_yt(client, user, dir):
    while not client.exists(dir):
        dir = dir.rsplit("/", 1)[0]
    return client.check_permission(user, "write", dir)["action"] == "allow"

def perform_precheck(task, clusters_configuration, logger):
    # TODO(ignat): add timeout for yt
    logger.info("Starting precheck")
    source_client = task.get_source_client(clusters_configuration.clusters)
    destination_client = task.get_destination_client(clusters_configuration.clusters)

    if task.copy_method not in [None, "proxy", "native"]:
        raise yt.YtError("Incorrect copy method: " + str(task.copy_method))

    if task.source_cluster not in clusters_configuration.availability_graph or \
       task.destination_cluster not in clusters_configuration.availability_graph[task.source_cluster]:
        raise yt.YtError("Cluster {0} not available from {1}".format(task.destination_cluster, task.source_cluster))

    if task.destination_table is None and destination_client._type != "kiwi":
        raise yt.YtError("Destination table should be specified for copying to {0}".format(destination_client._type))

    if source_client._type == "yt" and not source_client.exists(task.source_table):
        raise yt.YtError("Source table {0} is empty or does not exist".format(task.source_table))

    if source_client._type == "yt" and destination_client._type == "yt":
        if task.copy_method == "native" and get_cluster_version(source_client) != get_cluster_version(destination_client):
            raise yt.YtError("Native copy method should not be specified for clusters with "
                             "different YT versions")

    destination_exists = None
    if destination_client._type == "yt":
        destination_exists = destination_client.exists(task.destination_table)

    if destination_client._type == "yt":
        destination_dir = os.path.dirname(task.destination_table)
        # NB: copy operations usually create directories if it do not exits.
        #if not destination_client.exists(destination_dir):
        #    raise yt.YtError("Destination directory {} should exist".format(destination_dir))
        destination_user = destination_client.get_user_name(task.destination_cluster_token)
        if destination_user is None:
            raise yt.YtError("User is not authenticated. Please log in (supply correct token) to write to {0}".format(destination_dir))
        if not destination_exists and not _has_write_permission_on_yt(destination_client, destination_user, destination_dir):
            raise yt.YtError("User {0} has no permission to write to {1}. Make sure correct token is "
                             "supplied and ACLs are correctly configured".format(destination_user, destination_dir))

    if destination_client._type == "yt" and destination_exists and source_client._type == "yt":
        src_type = source_client.get(yt.TablePath(task.source_table, client=source_client) + "/@type")
        dst_type = destination_client.get(yt.TablePath(task.destination_table, client=destination_client) + "/@type")
        if src_type != dst_type:
            raise yt.YtError("Source '{0}' and destination '{1}' "
                             "must have the same type".format(task.source_table, task.destination_table))

    if destination_client._type == "kiwi" and clusters_configuration.kiwi_transmitter is None:
        raise yt.YtError("Transimission cluster for transfer to kiwi is not configured")

    if source_client._type == "hive" and "." not in task.source_table:
        raise yt.YtError("Incorrect source table for hive, it must have format {database_name}.{table_name}")

    if (source_client._type in ["hive", "hdfs", "hbase"]) and clusters_configuration.hadoop_transmitter is None:
        raise yt.YtError("Hadoop transmitter (airflow client) should be configured for transfer from {0} to {1}"
                         .format(source_client._type, destination_client._type))

    logger.info("Precheck completed")
