from helpers import get_cluster_version

import yt.logger as logger
import yt.wrapper as yt

import os
from subprocess32 import TimeoutExpired

def _has_write_permission_on_yt(client, user, dir):
    while not client.exists(dir):
        dir = dir.rsplit("/", 1)[0]
    return client.check_permission(user, "write", dir)["action"] == "allow"

def perform_precheck(task, clusters_configuration, ignore_timeout=False, yamr_timeout=None, custom_logger=None):
    if custom_logger is None:
        custom_logger = logger

    # TODO(ignat): add timeout for yt
    custom_logger.info("Starting precheck")
    source_client = task.get_source_client(clusters_configuration.clusters)
    destination_client = task.get_destination_client(clusters_configuration.clusters)

    if task.copy_method not in [None, "pull", "push", "proxy", "native"]:
        raise yt.YtError("Incorrect copy method: " + str(task.copy_method))

    if task.source_cluster not in clusters_configuration.availability_graph or \
       task.destination_cluster not in clusters_configuration.availability_graph[task.source_cluster]:
        raise yt.YtError("Cluster {0} not available from {1}".format(task.destination_cluster, task.source_cluster))

    if task.destination_table is None and destination_client._type != "kiwi":
        raise yt.YtError("Destination table should be specified for copying to {0}".format(destination_client._type))

    try:
        if yamr_timeout is not None:
            if source_client._type == "yamr":
                source_client._light_command_timeout = yamr_timeout
            if destination_client._type == "yamr":
                destination_client._light_command_timeout = yamr_timeout

        if (source_client._type == "yamr" and source_client.is_empty(task.source_table)) or \
           (source_client._type == "yt" and not source_client.exists(task.source_table)):
            raise yt.YtError("Source table {0} is empty or does not exist".format(task.source_table))

        if source_client._type == "yt" and destination_client._type == "yamr":
            if source_client.row_count(task.source_table) > 0:
                path = yt.TablePath(task.source_table, ranges=[{"upper_limit": {"row_index": 1}}], client=source_client)
                try:
                    stream = source_client.read_table(path, format=yt.YamrFormat(), raw=True)
                    stream.next()
                    stream.close()
                except yt.YtError as err:
                    raise yt.YtError("Failed to read first row in YAMR format", inner_errors=[err])

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

        if source_client._type == "yt" and source_client.get(yt.TablePath(task.source_table, client=source_client).name + "/@type") != "table":
            raise yt.YtError("Source '{0}' must be table".format(task.source_table))

        if destination_client._type == "yt" and destination_exists \
                and destination_client.get(yt.TablePath(task.destination_table, client=destination_client).name + "/@type") != "table":
            raise yt.YtError("Destination '{0}' must be table".format(task.destination_table))

        if destination_client._type == "kiwi" and clusters_configuration.kiwi_transmitter is None:
            raise yt.YtError("Transimission cluster for transfer to kiwi is not configured")

        if source_client._type == "hive" and "." not in task.source_table:
            raise yt.YtError("Incorrect source table for hive, it must have format {database_name}.{table_name}")

        if (source_client._type in ["hive", "hdfs", "hbase"]) and clusters_configuration.hadoop_transmitter is None:
            raise yt.YtError("Hadoop transmitter (airflow client) should be configured for transfer from {0} to {1}"
                             .format(source_client._type, destination_client._type))

        if task.skip_if_destination_exists and (
                (destination_client._type == "yamr" and not destination_client.is_empty(task.destination_table)) or \
                (destination_client._type == "yt" and destination_client.exists(task.destination_table))):
            task.state = "skipped"
    except TimeoutExpired:
        custom_logger.info("Precheck timed out")
        if not ignore_timeout:
            raise
        return

    custom_logger.info("Precheck completed")
