from yt.common import get_value

from yt.packages.six import iteritems

import yt.wrapper as yt

from copy import deepcopy

class Task(object):
    # NB: destination table is missing if we copy to kiwi
    def __init__(self, source_cluster, source_table, destination_cluster, creation_time, id, state, user,
                 destination_table=None, source_cluster_token=None, token=None, destination_cluster_token=None,
                 mr_user=None, error=None, start_time=None, restart_time=None, finish_time=None, copy_method=None,
                 skip_if_destination_exists=None, progress=None, history=None, backend_tag=None, kiwi_user=None,
                 kwworm_options=None, pool=None, meta=None, destination_compression_codec=None,
                 destination_erasure_codec=None, destination_force_sort=None, copy_spec=None, postprocess_spec=None,
                 job_timeout=None, intermediate_format=None, lease_timeout=None, queue_name=None,
                 force_copy_with_operation=None, external=None, additional_attributes=None, table_for_errors=None,
                 schema_inference_mode=None, temp_files_dir=None, hive_json_format_attributes=None,
                 data_proxy_role=None, logger=None, pack_yt_wrapper=None, pack_yson_bindings=None,
                 state_description=None):
        self.source_cluster = source_cluster
        self.source_table = source_table
        self.source_cluster_token = get_value(source_cluster_token, token)
        self.destination_cluster = destination_cluster
        self.destination_table = destination_table
        self.destination_cluster_token = get_value(destination_cluster_token, token)

        self.creation_time = creation_time
        self.start_time = start_time
        self.restart_time = restart_time
        self.finish_time = finish_time
        self.state = state
        self.id = id
        self.user = user
        self.mr_user = mr_user
        self.error = error
        self.token = token
        self.copy_method = copy_method
        self.skip_if_destination_exists = skip_if_destination_exists
        self.progress = progress
        self.history = history
        self.state_description = state_description

        self.backend_tag = backend_tag
        self.pool = pool
        self.kiwi_user = kiwi_user
        self.kwworm_options = kwworm_options
        self.copy_spec = copy_spec
        self.postprocess_spec = postprocess_spec
        self.intermediate_format = intermediate_format
        self.force_copy_with_operation = force_copy_with_operation
        self.external = external
        self.additional_attributes = additional_attributes
        self.table_for_errors = table_for_errors
        self.schema_inference_mode = schema_inference_mode
        self.temp_files_dir = temp_files_dir
        self.hive_json_format_attributes = hive_json_format_attributes
        self.data_proxy_role = data_proxy_role
        self.pack_yt_wrapper = pack_yt_wrapper
        self.pack_yson_bindings = pack_yson_bindings

        self.destination_compression_codec = destination_compression_codec
        self.destination_erasure_codec = destination_erasure_codec
        self.destination_force_sort = destination_force_sort

        self.lease_timeout = lease_timeout
        self.queue_name = queue_name

        # Special field to store meta-information for web-interface
        self.meta = meta

        self._last_ping_time = None
        self._subtasks = None

        self.logger = logger

    def get_direction_id(self):
        return self.queue_name, self.source_cluster, self.destination_cluster

    def get_direction_id_with_user(self):
        return self.queue_name, self.user, self.source_cluster, self.destination_cluster

    def get_source_client(self, clusters):
        if self.source_cluster not in clusters:
            raise yt.YtError("Unknown cluster " + self.source_cluster)
        client = deepcopy(clusters[self.source_cluster])
        if client._type == "yt":
            client.config["token"] = self.source_cluster_token
        return client

    def get_destination_client(self, clusters):
        if self.destination_cluster not in clusters:
            raise yt.YtError("Unknown cluster " + self.destination_cluster)
        client = deepcopy(clusters[self.destination_cluster])
        if client._type == "yt":
            client.config["token"] = self.destination_cluster_token
        return client

    def dict(self, hide_token=False, fields=None):
        result = self.__dict__.copy()
        del result["logger"]
        if hide_token:
            for key in ["token", "source_cluster_token", "destination_cluster_token"]:
                del result[key]
        if result["_subtasks"] is not None:
            result["subtasks"] = [subtask.dict() for subtask in result["_subtasks"]]
        for key in list(result):
            if result[key] is None or (fields is not None and key not in fields) or key.startswith("_"):
                del result[key]
        return result

    def copy(self):
        return Task(**dict((key, value) for key, value in iteritems(self.__dict__) if not key.startswith("_")))

    # Returns pair: active client, that run copy operation and passive client.
    def get_active_and_passive_clients(self, clusters_configuration):
        source_client = self.get_source_client(clusters_configuration.clusters)
        destination_client = self.get_destination_client(clusters_configuration.clusters)

        if destination_client._type == "yt":
            return destination_client, source_client
        elif destination_client._type == "kiwi":
            return clusters_configuration.kiwi_transmitter, source_client
        elif destination_client._type in ["hive", "hdfs", "hbase"]:
            return source_client, destination_client
        else:
            raise yt.YtError("Unknown destination client type: %r", destination_client._type)

class Subtask(object):
    def __init__(self, source_table, destination_table, start_time=None, finish_time=None, error=None, progress=None):
        self.source_table = source_table
        self.destination_table = destination_table
        self.start_time = start_time
        self.finish_time = finish_time
        self.error = error
        self.progress = None

    def dict(self):
        result = self.__dict__.copy()
        for key in list(result):
            if result[key] is None:
                del result[key]
