from .defaults import patch_defaults
from .spec_builder import get_clique_spec_builder
from .log_tailer import prepare_log_tailer_tables

from yt.wrapper.operation_commands import TimeWatcher, process_operation_unsuccesful_finish_state
from yt.wrapper.common import YtError, require, update, update_inplace
from yt.wrapper.run_operation_commands import run_operation
from yt.wrapper.cypress_commands import get, exists, copy, create
from yt.wrapper.transaction_commands import _make_transactional_request
from yt.wrapper.operation_commands import get_operation_url, abort_operation
from yt.wrapper.http_helpers import get_cluster_name
from yt.wrapper.file_commands import smart_upload_file
from yt.wrapper.config import get_config
from yt.wrapper.yson import dumps, to_yson_type

import yt.logger as logger

from yt.packages.six.moves import xrange

from copy import deepcopy
from tempfile import NamedTemporaryFile

import os
import json
import random


def _resolve_alias(operation_alias, client=None):
    if operation_alias is None:
        return None
    try:
        return json.loads(_make_transactional_request("get_operation", {
            "operation_alias": operation_alias,
            "include_runtime": True,
            "attributes": ["id", "state"]
        }, client=client))
    except:
        # TODO(max42): introduce error code.
        return None


def _format_url(url):
    return to_yson_type(url, attributes={"_type_tag": "url"})


@patch_defaults
def _build_description(cypress_ytserver_clickhouse_path=None,
                       cypress_ytserver_log_tailer_path=None,
                       cypress_clickhouse_trampoline_path=None,
                       artifact_path=None,
                       operation_alias=None,
                       prev_operation_id=None,
                       enable_monitoring=None,
                       client=None):

    description = {}
    if cypress_ytserver_clickhouse_path is not None:
        description = update(description, {"ytserver-clickhouse": get(cypress_ytserver_clickhouse_path + "/@user_attributes", client=client)})

    if cypress_ytserver_log_tailer_path is not None:
        description = update(description, {"ytserver-log-tailer": get(cypress_ytserver_log_tailer_path + "/@user_attributes", client=client)})

    if cypress_clickhouse_trampoline_path is not None:
        description = update(description, {"clickhouse-trampoline": get(cypress_clickhouse_trampoline_path + "/@user_attributes", client=client)})

    # Put information about previous incarnation of the operation by the given alias (if any).
    if prev_operation_id is not None:
        description["previous_operation_id"] = prev_operation_id
        description["previous_operation_url"] = _format_url(get_operation_url(prev_operation_id, client=client))

    cluster = get_cluster_name(client=client)

    # Put link to yql query. It is currently possible to add it only when alias is specified, otherwise we do not have access to operation id.
    # TODO(max42): YT-11115.
    if cluster is not None and operation_alias is not None:
        description["yql_url"] = _format_url(
            "https://yql.yandex-team.ru/?query=use%20chyt.{}/{}%3B%0A%0Aselect%201%3B&query_type=CLICKHOUSE"
                .format(cluster, operation_alias[1:]))

    # Put link to monitoring.
    if cluster is not None and operation_alias is not None and enable_monitoring:
        description["solomon_root_url"] = _format_url(
            "https://solomon.yandex-team.ru/?project=yt&cluster={}&service=yt_clickhouse&operation_alias={}"
                .format(cluster, operation_alias[1:]))
        description["solomon_dashboard_url"] = _format_url(
            "https://solomon.yandex-team.ru/?project=yt&cluster={}&service=yt_clickhouse&cookie=Aggr&dashboard=chyt&l.operation_alias={}"
                .format(cluster, operation_alias[1:]))

    return description


@patch_defaults
def prepare_configs(instance_count,
                    cypress_base_config_path=None,
                    cypress_log_tailer_config_path=None,
                    clickhouse_config=None,
                    cpu_limit=None,
                    memory_limit=None,
                    memory_footprint=None,
                    operation_alias=None,
                    uncompressed_block_cache_size=None,
                    client=None):
    """Merges a document pointed by `config_template_cypress_path`,  and `config` and uploads the
    result as a config.yson file suitable for specifying as a config file for clickhouse clique.

    :param cypress_base_config_path: path to a document that will be taken as a base config; if None, no base config is used
    :type cypress_base_config_path: str or None
    :param clickhouse_config: configuration patch to be applied onto the base config; if None, nothing happens
    :type clickhouse_config: dict or None
    :param uncompressed_block_cache_size: size of uncompressed block cache at each instance.
    :type int
    """

    require(cpu_limit is not None, lambda: YtError("Cpu limit should be set to prepare the ClickHouse config"))
    require(memory_limit is not None, lambda: YtError("Memory limit should be set to prepare the ClickHouse config"))

    clickhouse_config_base = {
        "engine": {
            "settings": {
                "max_threads": cpu_limit,
                "max_memory_usage_for_all_queries": memory_limit,
                "log_queries": 1,
                "queue_max_wait_ms": 30 * 1000,
            },
        },
        "memory_watchdog": {
            "memory_limit": memory_limit + uncompressed_block_cache_size + memory_footprint,
        },
        "profile_manager": {
            "global_tags": {"operation_alias": operation_alias[1:]} if operation_alias is not None else {},
        },
        "discovery": {
            "directory": "//sys/clickhouse/cliques",
        },
        "cluster_connection": {
            "block_cache": {
                "uncompressed_data": {
                    "capacity": uncompressed_block_cache_size,
                },
            },
        },
        "worker_thread_count": cpu_limit,
        "cpu_limit": cpu_limit,
    }

    cluster_connection_patch = {
        "cluster_connection": get("//sys/@cluster_connection", client=client)
    }

    clickhouse_config_cypress_base = get(cypress_base_config_path, client=client) if cypress_base_config_path else None
    resulting_clickhouse_config = {}
    for patch in (clickhouse_config_cypress_base, clickhouse_config_base, clickhouse_config, cluster_connection_patch):
        update_inplace(resulting_clickhouse_config, patch)

    log_tailer_config_base = {
        "profile_manager": {
            "global_tags": {"operation_alias": operation_alias[1:]} if operation_alias is not None else {},
        }
    }

    log_tailer_config_cypress_base = get(cypress_log_tailer_config_path, client=client) if cypress_log_tailer_config_path else None
    resulting_log_tailer_config = {}
    for patch in (log_tailer_config_cypress_base, log_tailer_config_base, cluster_connection_patch):
        update_inplace(resulting_log_tailer_config, patch)

    return {
        "clickhouse": resulting_clickhouse_config,
        "log_tailer": resulting_log_tailer_config,
    }


@patch_defaults
def prepare_artifacts(artifact_path,
                      prev_operation,
                      enable_job_tables=None,
                      dump_tables=None,
                      instance_count=None,
                      cypress_ytserver_log_tailer_path=None,
                      log_tailer_table_attribute_patch=None,
                      log_tailer_config=None,
                      log_tailer_tablet_count=None,
                      client=None):
    if not exists(artifact_path, client=client):
        logger.info("Creating artifact directory %s", artifact_path)
        create("map_node", artifact_path, client=client)

    if enable_job_tables:
        if not dump_tables:
            logger.warning("Job tables are enabled but dump_tables is not set; existing stderr and core tables may "
                           "be overridden")

        stderr_table_path = artifact_path + "/stderr_table"
        core_table_path = artifact_path + "/core_table"

        dump_suffix = None
        if dump_tables:
            if prev_operation is not None:
                dump_suffix = prev_operation["id"]
                name_collision = False
                for table_path in (stderr_table_path, core_table_path):
                    if exists(table_path + "." + dump_suffix, client=client):
                        name_collision = True
                if not name_collision:
                    logger.debug("Dumping suffix is previous operation id = %s", dump_suffix)
                else:
                    dump_suffix = None
            if dump_suffix is None:
                dump_suffix = ''.join(random.choice("0123456789abcdef") for i in xrange(16))
                logger.debug("Dumping suffix is random = %s", dump_suffix)

        for table_path in (stderr_table_path, core_table_path):
            if not exists(table_path, client=client):
                create("table", table_path, client=client)
            elif dump_tables:
                new_path = table_path + "." + dump_suffix
                logger.info("Dumping %s into %s", table_path, new_path)
                copy(table_path, new_path, client=client)

    log_tailer_version = get(cypress_ytserver_log_tailer_path + "/@yt_version") if cypress_ytserver_log_tailer_path else ""

    for log_file in log_tailer_config.get("log_tailer", {}).get("log_files", []):
        prepare_log_tailer_tables(log_file, artifact_path, log_tailer_version=log_tailer_version, client=client,
                                  attribute_patch=log_tailer_table_attribute_patch,
                                  tablet_count=log_tailer_tablet_count)


def upload_configs(configs, client=None):
    from yt.wrapper.client import YtClient
    patched_config = deepcopy(get_config(client))
    patched_config["remote_temp_files_directory"] = "//sys/clickhouse/kolkhoz/tmp"
    upload_client = YtClient(config=patched_config)

    cypress_config_paths = {}

    logger.info("Uploading configs")

    for config_key, filename in [("clickhouse", "config.yson"), ("log_tailer", "log_tailer_config.yson")]:
        # We do not use NamedTemporaryFile as a context manager intentionally:
        # due to weird Windows behaviour one cannot re-open temporary file by
        # temp_file.name while holding temp_file.
        # See also: https://docs.python.org/2/library/tempfile.html#tempfile.NamedTemporaryFile.
        temp_file = NamedTemporaryFile(delete=False)
        temp_file.write(dumps(configs[config_key], yson_format="pretty"))
        temp_file.close()
        logger.debug("Uploading config for %s", config_key)
        cypress_path = smart_upload_file(temp_file.name, client=upload_client)
        cypress_config_paths[config_key] = (cypress_path, filename)
        os.unlink(temp_file.name)

    logger.info("Configs uploaded")
    logger.debug("Cypress config paths: %s", cypress_config_paths)

    return cypress_config_paths


def do_wait_for_instances(op, instance_count, operation_alias, client=None):
    for state in op.get_state_monitor(TimeWatcher(1.0, 1.0, 0.0)):
        if state.is_running() and \
                exists("//sys/clickhouse/cliques/{0}".format(op.id), client=client) and \
                get("//sys/clickhouse/cliques/{0}/@count".format(op.id), client=client) == instance_count:
            logger.info("Clique started and ready for serving under alias %s", operation_alias)
            return op
        elif state.is_unsuccessfully_finished():
            process_operation_unsuccesful_finish_state(op, state)
        else:
            op.printer(state)


def start_clique(instance_count,
                 operation_alias,
                 cypress_base_config_path=None,
                 cypress_ytserver_clickhouse_path=None,
                 cypress_clickhouse_trampoline_path=None,
                 cypress_ytserver_log_tailer_path=None,
                 host_ytserver_clickhouse_path=None,
                 host_clickhouse_trampoline_path=None,
                 host_ytserver_log_tailer_path=None,
                 clickhouse_config=None,
                 cpu_limit=None,
                 memory_limit=None,
                 memory_footprint=None,
                 enable_monitoring=None,
                 cypress_geodata_path=None,
                 description=None,
                 abort_existing=None,
                 dump_tables=None,
                 spec=None,
                 uncompressed_block_cache_size=None,
                 cypress_log_tailer_config_path=None,
                 enable_job_tables=None,
                 artifact_path=None,
                 client=None,
                 wait_for_instances=None,
                 **kwargs):
    """Starts a clickhouse clique consisting of a given number of instances.

    :param operation_alias alias for the underlying YT operation
    :type operation_alias: str
    :param cypress_base_config_path: path for the base clickhouse config in Cypress
    :type cypress_base_config_path: str or None
    :param cypress_ytserver_clickhouse_path path to the ytserver-clickhouse binary in Cypress
    :type cypress_ytserver_clickhouse_path: str or None
    :param cypress_clickhouse_trampoline_path: path to the clickhouse-trampoline binary in Cypress
    :type cypress_clickhouse_trampoline_path: str or None
    :param cypress_ytserver_log_tailer_path: path to the ytserver-log-tailer binary in Cypress
    :type cypress_ytserver_log_tailer_path: str or None
    :param host_ytserver_clickhouse_path path to the ytserver-clickhouse binary on the destination host (useful for
    integration tests)
    :type host_ytserver_clickhouse_path: str or None
    :param host_clickhouse_trampoline_path: path to the clickhouse-trampoline binary on the destination host (useful for
    integration tests)
    :type host_clickhouse_trampoline_path: str or None
    :param host_ytserver_log_tailer_path: path to the ytserver-log-tailer binary on the destination host (useful for
    integration tests)
    :type host_ytserver_clickhouse_path: str or None
    :param instance_count: number of instances (also the number of jobs in the underlying vanilla operation).
    :type instance_count: int or None
    :param clickhouse_config: patch to be applied to clickhouse config.
    :type clickhouse_config: dict or None
    :param cpu_limit: number of cores that will be available to each instance
    :type cpu_limit: int or None
    :param memory_limit: amount of memory that will be available to each instance
    :type memory_limit: int or None
    :param memory_footprint: amount of memory that goes to the YT runtime
    :type memory_footprint: int or None
    :param enable_monitoring: (only for development use) option that makes clickhouse bind monitoring port to 10042.
    :type enable_monitoring: bool or None
    :param dump_tables: if stderr and/or core tables are specified, copy their incarnations from the previous operation
    to separate tables in order not to rewrite them
    :type dump_tables: bool or None
    :param description: YSON document which will be placed in corresponding operation description.
    :type description: str or None
    :param spec: additional operation spec
    :type spec: dict or None
    :param abort_existing: Should we abort the existing operation with the given alias?
    :type abort_existing: bool or None
    :param cypress_log_tailer_config_path: path for the log tailer config in Cypress
    :type cypress_log_tailer_config_path: str or None
    :param enable_job_tables: enable core and stderr tables
    :type enable_job_tables: bool or None
    :param artifact_path: path for artifact directory; by default equals to //sys/clickhouse/kolkhoz/<operation_alias>
    :type artifact_path: str or None
    :param cypress_geodata_path: path to archive with geodata in Cypress
    :type cypress_geodata_path str or None
    .. seealso::  :ref:`operation_parameters`.
    """

    if exists("//sys/clickhouse/defaults", client=client):
        defaults = get("//sys/clickhouse/defaults", client=client)
    else:
        raise YtError("CHYT is not set up on cluster: //sys/clickhouse/defaults not found")

    require(operation_alias.startswith("*"), lambda: YtError("Operation alias should start with '*' character"))

    artifact_path = artifact_path or "//home/clickhouse-kolkhoz/" + operation_alias[1:]

    if abort_existing is None:
        abort_existing = False

    prev_operation = _resolve_alias(operation_alias, client=client)
    if operation_alias is not None:
        if prev_operation is not None:
            logger.info("Previous operation with alias %s is %s with state %s", operation_alias, prev_operation["id"], prev_operation["state"])
            if not abort_existing:
                raise YtError("There is already an operation with alias {}; abort it or specify --abort-existing command-line flag".format(operation_alias))
        else:
            logger.info("There was no operation with alias %s before", operation_alias)

    if abort_existing:
        if prev_operation is not None and not prev_operation["state"].endswith("ed"):
            logger.info("Aborting previous operation with alias %s", operation_alias)
            abort_operation(prev_operation["id"], client=client)
        else:
            logger.info("There is no running operation with alias %s; not aborting anything", operation_alias)

    prev_operation_id = prev_operation["id"] if prev_operation is not None else None

    def resolve_path(cypress_bin_path, host_bin_path, bin_name, client=None):
        host_bin_path = host_bin_path or defaults.get("host_" + bin_name.replace("-", "_") + "_path")
        if cypress_bin_path is None and host_bin_path is None:
            cypress_bin_path = get("//sys/bin/{0}/{0}/@path".format(bin_name), client=client)
        require(cypress_bin_path is None or host_bin_path is None,
                lambda: YtError("Cypress {0} binary path and host {0} path "
                                "cannot be specified at the same time").format(bin_name))
        return cypress_bin_path, host_bin_path

    cypress_ytserver_clickhouse_path, host_ytserver_clickhouse_path = \
        resolve_path(cypress_ytserver_clickhouse_path, host_ytserver_clickhouse_path, "ytserver-clickhouse", client=client)

    cypress_clickhouse_trampoline_path, host_clickhouse_trampoline_path = \
        resolve_path(cypress_clickhouse_trampoline_path, host_clickhouse_trampoline_path, "clickhouse-trampoline", client=client)

    cypress_ytserver_log_tailer_path, host_ytserver_log_tailer_path = \
        resolve_path(cypress_ytserver_log_tailer_path, host_ytserver_log_tailer_path, "ytserver-log-tailer", client=client)

    if wait_for_instances is None:
        wait_for_instances = True

    configs = prepare_configs(instance_count,
                              cypress_base_config_path=cypress_base_config_path,
                              cypress_log_tailer_config_path=cypress_log_tailer_config_path,
                              clickhouse_config=clickhouse_config,
                              cpu_limit=cpu_limit,
                              memory_limit=memory_limit,
                              defaults=defaults,
                              operation_alias=operation_alias,
                              uncompressed_block_cache_size=uncompressed_block_cache_size,
                              client=client)

    prepare_artifacts(artifact_path,
                      prev_operation,
                      enable_job_tables=enable_job_tables,
                      dump_tables=dump_tables,
                      defaults=defaults,
                      cypress_ytserver_log_tailer_path=cypress_ytserver_log_tailer_path,
                      instance_count=instance_count,
                      log_tailer_config=configs["log_tailer"],
                      client=client)

    cypress_config_paths = upload_configs(configs, client=client)

    description = update(description, _build_description(cypress_ytserver_clickhouse_path=cypress_ytserver_clickhouse_path,
                                                         cypress_ytserver_log_tailer_path=cypress_ytserver_log_tailer_path,
                                                         cypress_clickhouse_trampoline_path=cypress_clickhouse_trampoline_path,
                                                         operation_alias=operation_alias,
                                                         prev_operation_id=prev_operation_id,
                                                         enable_monitoring=enable_monitoring,
                                                         defaults=defaults,
                                                         client=client))

    op = run_operation(get_clique_spec_builder(instance_count,
                                               artifact_path=artifact_path,
                                               cypress_config_paths=cypress_config_paths,
                                               cpu_limit=cpu_limit,
                                               memory_limit=memory_limit,
                                               memory_footprint=memory_footprint,
                                               enable_monitoring=enable_monitoring,
                                               cypress_ytserver_clickhouse_path=cypress_ytserver_clickhouse_path,
                                               cypress_clickhouse_trampoline_path=cypress_clickhouse_trampoline_path,
                                               cypress_ytserver_log_tailer_path=cypress_ytserver_log_tailer_path,
                                               host_ytserver_clickhouse_path=host_ytserver_clickhouse_path,
                                               host_clickhouse_trampoline_path=host_clickhouse_trampoline_path,
                                               host_ytserver_log_tailer_path=host_ytserver_log_tailer_path,
                                               cypress_geodata_path=cypress_geodata_path,
                                               operation_alias=operation_alias,
                                               description=description,
                                               uncompressed_block_cache_size=uncompressed_block_cache_size,
                                               spec=spec,
                                               enable_job_tables=enable_job_tables,
                                               enable_log_tailer=True,
                                               defaults=defaults,
                                               **kwargs),
                       client=client,
                       sync=False)

    if wait_for_instances:
        do_wait_for_instances(op, instance_count, operation_alias, client=client)
