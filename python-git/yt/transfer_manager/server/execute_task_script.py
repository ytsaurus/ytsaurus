from yt.transfer_manager.server import traceback_helpers
from yt.transfer_manager.server.task_executor import MessageWriter
from yt.transfer_manager.server.task_types import Task
from yt.transfer_manager.server.logger import TaskIdLogger
from yt.transfer_manager.server.helpers import log_yt_exception, configure_logger
from yt.transfer_manager.server.precheck import perform_precheck
from yt.transfer_manager.server.clusters_configuration import get_clusters_configuration_from_config
from yt.transfer_manager.server.remote_copy_tools import (
    copy_yt_to_kiwi,
    copy_yt_to_yt,
    copy_yt_to_yt_through_proxy,
    copy_file_yt_to_yt,
    copy_hive_to_yt,
    copy_hadoop_to_hadoop_with_airflow)

import yt.logger as yt_logger
from yt.wrapper.common import update, get_value
import yt.json as json
import yt.wrapper as yt

import os
import sys
import time
import logging
import traceback
import argparse
from copy import deepcopy

def _truncate_stderrs_attributes(error, limit):
    if hasattr(error, "attributes") and "stderrs" in error.attributes:
        if isinstance(error, yt.YtOperationFailedError):
            error.attributes["details"] = yt.format_operation_stderrs(error.attributes["stderrs"])[:limit]
        del error.attributes["stderrs"]
    if hasattr(error, "inner_errors"):
        for inner_error in error.inner_errors:
            _truncate_stderrs_attributes(inner_error, limit)

def execute_task(task, message_queue, config, logger):
    logger.info("Start executing task (pid %s)", os.getpid())
    try:
        clusters_configuration = get_clusters_configuration_from_config(config)
        perform_precheck(task, clusters_configuration, logger)

        title = "Supervised by transfer task " + task.id

        common_spec = {
            "title": title,
            "transfer_manager": {
                "task_id": task.id,
                "backend_tag": task.backend_tag,
                "source_cluster": task.source_cluster,
                "destination_cluster": task.destination_cluster
            }
        }

        base_operation_spec = {
            "pool": task.pool,
            "resource_limits": {
                "user_slots": config.get("user_slots_limit_per_operation", 200)
            }
        }

        copy_spec = update(base_operation_spec, update(deepcopy(common_spec), get_value(task.copy_spec, {})))
        postprocess_spec = update(deepcopy(common_spec), get_value(task.postprocess_spec, {}))

        source_client = task.get_source_client(clusters_configuration.clusters)
        source_client.message_queue = message_queue

        destination_client = task.get_destination_client(clusters_configuration.clusters)
        destination_client.message_queue = message_queue

        clusters_configuration.kiwi_transmitter.message_queue = message_queue
        clusters_configuration.hadoop_transmitter.message_queue = message_queue

        parameters = clusters_configuration.availability_graph[task.source_cluster][task.destination_cluster]

        # Calculate fastbone
        fastbone = source_client._parameters.get("fastbone", False) and destination_client._parameters.get("fastbone", False)
        fastbone = parameters.get("fastbone", fastbone)

        data_proxy_role = source_client._parameters.get("data_proxy_role")
        data_proxy_role = get_value(task.data_proxy_role, parameters.get("data_proxy_role", data_proxy_role))

        if data_proxy_role is None and fastbone:
            data_proxy_role = "fb"

        force_copy_with_operation = task.force_copy_with_operation or \
                not config.get("enable_copy_without_operation", True)


        if source_client._type == "yt" and destination_client._type == "yt":
            logger.info("Running YT -> YT remote copy operation")
            if source_client.get(yt.YPath(task.source_table, client=source_client).to_yson_type() + "/@type") == "file":
                copy_file_yt_to_yt(
                    source_client,
                    destination_client,
                    task.source_table,
                    task.destination_table,
                    data_proxy_role=data_proxy_role,
                    copy_spec_template=copy_spec,
                    compression_codec=task.destination_compression_codec,
                    erasure_codec=task.destination_erasure_codec,
                    intermediate_format=task.intermediate_format,
                    default_tmp_dir=config.get("default_tmp_dir"),
                    small_file_size_threshold=config.get("small_table_size_threshold"),
                    force_copy_with_operation=force_copy_with_operation,
                    additional_attributes=task.additional_attributes,
                    temp_files_dir=task.temp_files_dir,
                    pack_yt_wrapper=task.pack_yt_wrapper,
                    pack_yson_bindings=task.pack_yson_bindings)
            elif task.copy_method == "proxy":
                copy_yt_to_yt_through_proxy(
                    source_client,
                    destination_client,
                    task.source_table,
                    task.destination_table,
                    data_proxy_role=data_proxy_role,
                    copy_spec_template=copy_spec,
                    postprocess_spec_template=postprocess_spec,
                    compression_codec=task.destination_compression_codec,
                    erasure_codec=task.destination_erasure_codec,
                    intermediate_format=task.intermediate_format,
                    default_tmp_dir=config.get("default_tmp_dir"),
                    small_table_size_threshold=config.get("small_table_size_threshold"),
                    force_copy_with_operation=force_copy_with_operation,
                    external=task.external,
                    additional_attributes=task.additional_attributes,
                    schema_inference_mode=task.schema_inference_mode,
                    pack_yt_wrapper=task.pack_yt_wrapper,
                    pack_yson_bindings=task.pack_yson_bindings)
            else:  # native
                network_name = "fastbone" if fastbone else "default"
                network_name = parameters.get("network_name", network_name)
                copy_yt_to_yt(
                    source_client,
                    destination_client,
                    task.source_table,
                    task.destination_table,
                    network_name=network_name,
                    copy_spec_template=copy_spec,
                    postprocess_spec_template=postprocess_spec,
                    compression_codec=task.destination_compression_codec,
                    erasure_codec=task.destination_erasure_codec,
                    external=task.external,
                    additional_attributes=task.additional_attributes,
                    schema_inference_mode=task.schema_inference_mode)
        elif source_client._type == "yt" and destination_client._type == "kiwi":
            dc_name = source_client._parameters.get("dc_name")
            if dc_name is not None:
                copy_spec = update({"scheduling_tag": dc_name}, copy_spec)
            copy_yt_to_kiwi(
                source_client,
                destination_client,
                clusters_configuration.kiwi_transmitter,
                task.source_table,
                data_proxy_role=data_proxy_role,
                kiwi_user=task.kiwi_user,
                kwworm_options=task.kwworm_options,
                copy_spec_template=copy_spec,
                table_for_errors=task.table_for_errors,
                default_tmp_dir=config.get("default_tmp_dir"),
                pack_yt_wrapper=task.pack_yt_wrapper,
                pack_yson_bindings=task.pack_yson_bindings)
        elif source_client._type == "hive" and destination_client._type == "yt":
            copy_hive_to_yt(
                source_client,
                destination_client,
                task.source_table,
                task.destination_table,
                copy_spec_template=copy_spec,
                postprocess_spec_template=postprocess_spec,
                compression_codec=task.destination_compression_codec,
                erasure_codec=task.destination_erasure_codec,
                json_format_attributes=task.hive_json_format_attributes)
        elif (source_client._type == "hdfs" and destination_client._type == "hdfs") \
                or (source_client._type == "hive" and destination_client._type == "hive") \
                or (source_client._type == "hbase" and destination_client._type == "hbase"):
            type_to_task_type = {"hdfs": "distcp", "hive": "hivecp", "hbase": "hbasecp"}
            copy_hadoop_to_hadoop_with_airflow(
                type_to_task_type[source_client._type],
                clusters_configuration.hadoop_transmitter,
                task.source_table,
                source_client.airflow_name,
                task.destination_table,
                destination_client.airflow_name,
                task.user)
        else:
            raise Exception("Incorrect cluster types: {} source and {} destination".format(
                            source_client._type,
                            destination_client._type))

        logger.info("Task completed")
        message_queue.put({"type": "completed"})

    except yt.YtError as error:
        try:
            _truncate_stderrs_attributes(error, config["error_details_length_limit"])
        except Exception:
            logger.exception("Task failed with error:")
            raise
        log_yt_exception(logger, "Task {} failed with error:".format(task.id))
        message_queue.put({
            "type": "error",
            "error": error.simplify()
        })
    except BaseException as error:
        logger.exception("Task failed with error:")
        message_queue.put({
            "type": "error",
            "error": {
                "message": str(error),
                "code": 1,
                "attributes": {
                    "details": (traceback_helpers.format_exc() if config["enable_detailed_traceback"] else traceback.format_exc())
                }
            }
        })

    # NB: hack to avoid process died silently.
    time.sleep(config["execute_task_grace_termination_sleep_timeout"] / 1000.0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transfer Manager executor")
    parser.add_argument("--config-path", required=True)
    parser.add_argument("--task-executor-address", required=True)
    args = parser.parse_args()

    with open(args.config_path, "rb") as f:
        config = json.load(f)
    task = Task(**json.loads_as_bytes(sys.stdin.read().strip()))
    message_queue = MessageWriter(args.task_executor_address, task.id)

    configure_logger(config["logging"])
    logger = TaskIdLogger(logging.getLogger("TM.task"), task.id)
    yt_logger.LOGGER = TaskIdLogger(logging.getLogger("TM.task.yt"), task.id)

    execute_task(task, message_queue, config, logger)
