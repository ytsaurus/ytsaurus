from .operation_commands import TimeWatcher, process_operation_unsuccesful_finish_state
from .common import YtError, require, update, update_inplace
from .spec_builders import VanillaSpecBuilder
from .run_operation_commands import run_operation
from .dynamic_table_commands import mount_table, unmount_table, reshard_table
from .cypress_commands import get, exists, copy, create, set
from .transaction_commands import _make_transactional_request
from .operation_commands import get_operation_url, abort_operation
from .http_helpers import get_cluster_name
from .ypath import FilePath
from .file_commands import smart_upload_file
from .config import get_config
from .yson import dumps, to_yson_type

import yt.logger as logger

from yt.packages.six import iteritems, itervalues, PY3
from yt.packages.six.moves import xrange
from yt.yson import YsonUint64

from copy import deepcopy
from tempfile import NamedTemporaryFile

import os.path
import json
import random
import inspect

CYPRESS_DEFAULTS_PATH = "//sys/clickhouse/defaults"
BUNDLED_DEFAULTS = {
    "memory_footprint": 16 * 1000**3,
    "memory_limit": 15 * 1000**3,
    "cypress_base_config_path": "//sys/clickhouse/config",
    "cypress_log_tailer_config_path": "//sys/clickhouse/log_tailer_config",
    "dump_tables": True,
    "cpu_limit": 8,
    "enable_monitoring": True,
    "clickhouse_config": {},
    "max_failed_job_count": 10 * 1000,
    "uncompressed_block_cache_size": 20 * 1000*3,
    "max_instance_count": 100,
}


def _is_fresher_than(version, min_version):
    if version is None:
        return False
    return version >= min_version


def _get_kwargs_names(fn):
    if PY3:
        argspec = inspect.getfullargspec(fn)
    else:
        argspec = inspect.getargspec(fn)
    kwargs_len = len(argspec.defaults)
    kwargs_names = argspec.args[-kwargs_len:]
    return kwargs_names


def _patch_defaults(fn):
    kwargs_names = _get_kwargs_names(fn)

    def wrapped_fn(*args, **kwargs):
        defaults_dict = kwargs.pop("defaults")
        logger.debug("Applying following argument defaults: %s", defaults_dict)
        for key, default_value in iteritems(defaults_dict):
            if key in kwargs_names:
                current_value = kwargs.get(key)
                if current_value is None:
                    kwargs[key] = default_value
        logger.debug("Resulting arguments: %s", kwargs)
        return fn(*args, **kwargs)

    wrapped_fn.__doc__ = fn.__doc__

    return wrapped_fn


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


@_patch_defaults
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

@_patch_defaults
def get_clickhouse_clique_spec_builder(instance_count,
                                       artifact_path=None,
                                       cypress_ytserver_clickhouse_path=None,
                                       cypress_clickhouse_trampoline_path=None,
                                       cypress_ytserver_log_tailer_path=None,
                                       host_ytserver_clickhouse_path=None,
                                       host_clickhouse_trampoline_path=None,
                                       host_ytserver_log_tailer_path=None,
                                       cypress_config_paths=None,
                                       max_failed_job_count=None,
                                       cpu_limit=None,
                                       memory_limit=None,
                                       memory_footprint=None,
                                       enable_monitoring=None,
                                       cypress_geodata_path=None,
                                       core_dump_destination=None,
                                       description=None,
                                       operation_alias=None,
                                       enable_job_tables=None,
                                       enable_log_tailer=None,
                                       uncompressed_block_cache_size=None,
                                       trampoline_log_file=None,
                                       max_instance_count=None,
                                       spec=None):
    """Returns a spec builder for the clickhouse clique consisting of a given number of instances.

    :param instance_count: number of instances (also the number of jobs in the underlying vanilla operation).
    :type instance_count: int
    :param cypress_ytserver_clickhouse_path: path to the ytserver-clickhouse binary in Cypress or None.
    :type cypress_ytserver_clickhouse_path: str
    :param host_ytserver_clickhouse_path: path to the ytserver-clickhouse binary on the destination node or None.
    :type host_ytserver_clickhouse_path: str
    :param max_failed_job_count: maximum number of failed jobs that is allowed for the underlying vanilla operation.
    :type max_failed_job_count: int
    :param memory_footprint: amount of memory that goes to the YT runtime
    :type memory_footprint: int
    :param enable_monitoring: (only for development use) option that makes clickhouse bind monitoring port to 10042.
    :type enable_monitoring: bool
    :param spec: other spec options.
    :type spec: dict

    .. seealso::  :ref:`operation_parameters`.
    """

    require(cypress_config_paths is not None,
            lambda: YtError("At least cypress clickhouse server config.yson path should be specified as cypress_config_"
                            "paths dictionary; consider using prepare_cypress_configs helper"))

    require(instance_count <= max_instance_count,
            lambda: YtError("Requested instance count exceeds maximum allowed instance count: {} > {}; if you indeed want to run clique of such size, "
                            "consult with CHYT support in support chat".format(instance_count, max_instance_count)))

    spec_base = {
        "annotations": {
            "is_clique": True,
            "expose": True,
        },
        "tasks": {
            "instances": {
                "user_job_memory_digest_lower_bound": 1.0,
                "restart_completed_jobs": True,
                "interruption_signal": "SIGINT",
            }
        },
    }

    stderr_table_path = None
    core_table_path = None

    if enable_job_tables:
        stderr_table_path = artifact_path + "/stderr_table"
        core_table_path = artifact_path + "/core_table"

    spec = update(spec_base, spec)

    file_paths = [FilePath(cypress_config_path, file_name=file_name) for cypress_config_path, file_name
                  in itervalues(cypress_config_paths)]

    def add_file(cypress_bin_path, host_bin_path, bin_name):
        if cypress_bin_path is not None:
            file_paths.append(FilePath(cypress_bin_path, file_name=bin_name))
            return "./" + bin_name
        elif host_bin_path is not None:
            return host_bin_path
        else:
            return None

    ytserver_clickhouse_path = add_file(cypress_ytserver_clickhouse_path, host_ytserver_clickhouse_path, "ytserver-clickhouse")
    clickhouse_trampoline_path = add_file(cypress_clickhouse_trampoline_path, host_clickhouse_trampoline_path, "clickhouse-trampoline")
    ytserver_log_tailer_path = add_file(cypress_ytserver_log_tailer_path, host_ytserver_log_tailer_path, "ytserver-log-tailer")

    args = [clickhouse_trampoline_path, ytserver_clickhouse_path]
    if enable_monitoring:
        args += ["--monitoring-port", "10142", "--log-tailer-monitoring-port", "10242"]

    if cypress_geodata_path is not None:
        file_paths.append(FilePath(cypress_geodata_path, file_name="geodata.tgz"))
        args += ["--prepare-geodata"]

    if core_dump_destination is not None:
        args += ["--core-dump-destination", core_dump_destination]

    if enable_log_tailer:
        args += ["--log-tailer-bin", ytserver_log_tailer_path]

    if trampoline_log_file:
        args += ["--log-file", trampoline_log_file]

    trampoline_command = " ".join(args)

    spec_builder = \
        VanillaSpecBuilder() \
            .begin_task("instances") \
                .job_count(instance_count) \
                .file_paths(file_paths) \
                .command(trampoline_command) \
                .memory_limit(memory_limit + memory_footprint + uncompressed_block_cache_size) \
                .cpu_limit(cpu_limit) \
                .max_stderr_size(1024 * 1024 * 1024) \
                .port_count(5) \
            .end_task() \
            .max_failed_job_count(max_failed_job_count) \
            .description(description) \
            .max_stderr_count(150) \
            .stderr_table_path(stderr_table_path) \
            .core_table_path(core_table_path) \
            .alias(operation_alias) \
            .spec(spec)

    if "pool" not in spec_builder.build():
        logger.warning("It is discouraged to run clique in ephemeral pool "
                       "(which happens when pool is not specified explicitly)")

    return spec_builder


@_patch_defaults
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

    clickhouse_config_cypress_base = get(cypress_base_config_path, client=client) if cypress_base_config_path != "" else None
    resulting_clickhouse_config = {}
    for patch in (clickhouse_config_cypress_base, clickhouse_config_base, clickhouse_config, cluster_connection_patch):
        update_inplace(resulting_clickhouse_config, patch)

    log_tailer_config_base = {
        "profile_manager": {
            "global_tags": {"operation_alias": operation_alias[1:]} if operation_alias is not None else {},
        }
    }

    log_tailer_config_cypress_base = get(cypress_log_tailer_config_path, client=client) if cypress_log_tailer_config_path != "" else None
    resulting_log_tailer_config = {}
    for patch in (log_tailer_config_cypress_base, log_tailer_config_base, cluster_connection_patch):
        update_inplace(resulting_log_tailer_config, patch)

    return {
        "clickhouse": resulting_clickhouse_config,
        "log_tailer": resulting_log_tailer_config,
    }


# Here and below table_kind is in ("ordered_normally", "ordered_by_trace_id")

def set_log_tailer_table_attributes(table_kind, table_path, ttl, log_tailer_version=None, client=None):
    # COMPAT(max42)
    atomicity = "none" if _is_fresher_than(log_tailer_version, "19.8.34144") else "full"

    attributes = {
        "min_data_versions": 0,
        "max_data_versions": 1,
        "max_dynamic_store_pool_size": 268435456,
        "min_data_ttl": ttl,
        "max_data_ttl": ttl,
        "primary_medium": "ssd_blobs",
        "optimize_for": "scan",
        "backing_store_retention_time": 0,
        "auto_compaction_period": 86400000,
        "dynamic_store_overflow_threshold": 0.5,
        "merge_rows_on_flush": True,
        "atomicity": atomicity,
    }

    if log_tailer_version is not None:
        attributes["log_tailer_version"] = log_tailer_version

    for attribute, value in iteritems(attributes):
        attribute_path = table_path + "/@" + attribute
        logger.debug("Setting %s to %s", attribute_path, value)
        set(attribute_path, value, client=client)


def set_log_tailer_table_dynamic_attributes(table_kind, table_path, client=None):
    attributes = {
        "tablet_balancer_config/min_tablet_size": 0,
        "tablet_balancer_config/desired_tablet_size": 10 * 1024**3,
        "tablet_balancer_config/max_tablet_size": 20 * 1024**3,
    }
    for attribute, value in iteritems(attributes):
        attribute_path = table_path + "/@" + attribute
        logger.debug("Setting %s to %s", attribute_path, value)
        set(attribute_path, value, client=client)


def reshard_log_tailer_table(table_kind, table_path, sync=True, client=None):
    if table_kind == "ordered_normally":
        logger.debug("Resharding %s", table_path)
    pivot_keys = [[]] + [[YsonUint64(i), None, None, None] for i in xrange(1, 100)]
    reshard_table(table_path, pivot_keys=pivot_keys, sync=sync, client=client)


def create_log_tailer_table(table_kind, table_path, client=None):
    ORDERED_NORMALLY_SCHEMA = [
        {"name": "job_id_shard", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(job_id) % 100"},
        {"name": "timestamp", "type": "string", "sort_order": "ascending"},
        {"name": "job_id", "type": "string", "sort_order": "ascending"},
        {"name": "increment", "type": "uint64", "sort_order": "ascending"},
        {"name": "category", "type": "string"},
        {"name": "message", "type": "string"},
        {"name": "log_level", "type": "string"},
        {"name": "thread_id", "type": "string"},
        {"name": "fiber_id", "type": "string"},
        {"name": "trace_id", "type": "string"},
        {"name": "operation_id", "type": "string"}
    ]
    ORDERED_BY_TRACE_ID_SCHEMA = [
        {"name": "trace_id_hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(trace_id)"},
        {"name": "trace_id", "type": "string", "sort_order": "ascending"},
        {"name": "timestamp", "type": "string", "sort_order": "ascending"},
        {"name": "job_id", "type": "string", "sort_order": "ascending"},
        {"name": "increment", "type": "uint64", "sort_order": "ascending"},
        {"name": "category", "type": "string"},
        {"name": "message", "type": "string"},
        {"name": "log_level", "type": "string"},
        {"name": "thread_id", "type": "string"},
        {"name": "fiber_id", "type": "string"},
        {"name": "operation_id", "type": "string"}
    ]

    table_kind_to_schema = {"ordered_normally": ORDERED_NORMALLY_SCHEMA, "ordered_by_trace_id": ORDERED_BY_TRACE_ID_SCHEMA}
    schema = table_kind_to_schema[table_kind]
    attributes = {
        "dynamic": True,
        "schema": schema,
    }
    logger.info("Creating log tailer table %s of kind %s", table_path, table_kind)
    create("table", table_path, attributes=attributes, client=client, force=True)



def prepare_log_tailer_tables(log_file,
                              artifact_path,
                              log_tailer_version=None,
                              instance_count=None,
                              client=None):

    assert len(log_file.get("tables", [])) == 0

    base_path = os.path.basename(log_file["path"])

    ordered_normally_path = artifact_path + "/" + base_path
    ordered_by_trace_id_path = artifact_path + "/" + base_path + ".ordered_by_trace_id"

    log_file["tables"] = [
        {"path": ordered_normally_path},
        {"path": ordered_by_trace_id_path, "require_trace_id": True},
    ]

    ttl = log_file["ttl"]

    for kind, path in [("ordered_normally", ordered_normally_path), ("ordered_by_trace_id", ordered_by_trace_id_path)]:
        logger.info("Preparing log table %s", path)
        if not exists(path, client=client):
            create_log_tailer_table(kind, path, client=client)
        else:
            unmount_table(path, sync=True)
        set_log_tailer_table_attributes(kind, path, ttl, log_tailer_version=log_tailer_version, client=client)
        set_log_tailer_table_dynamic_attributes(kind, path, client=client)
        reshard_log_tailer_table(kind, path, client=client)
        mount_table(path, sync=True)


@_patch_defaults
def prepare_artifacts(artifact_path,
                      prev_operation,
                      enable_job_tables=None,
                      dump_tables=None,
                      instance_count=None,
                      cypress_ytserver_log_tailer_path=None,
                      log_tailer_config=None,
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

    log_tailer_version = get(cypress_ytserver_log_tailer_path + "/@yt_version")

    for log_file in log_tailer_config["log_tailer"]["log_files"]:
        prepare_log_tailer_tables(log_file, artifact_path, log_tailer_version=log_tailer_version, instance_count=instance_count, client=client)


def upload_configs(configs, client=None):
    def create_client_with_clickhouse_tmp_directory(client):
        from .client import YtClient
        patched_config = deepcopy(get_config(client))
        patched_config["remote_temp_files_directory"] = "//sys/clickhouse/kolkhoz/tmp"
        return YtClient(config=patched_config)

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
        cypress_path = smart_upload_file(temp_file.name, client=create_client_with_clickhouse_tmp_directory(client))
        cypress_config_paths[config_key] = (cypress_path, filename)
        os.unlink(temp_file.name)

    logger.info("Configs uploaded")
    logger.debug("Cypress config paths: %s", cypress_config_paths)

    return cypress_config_paths


def start_clickhouse_clique(instance_count,
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

    defaults = BUNDLED_DEFAULTS
    if exists("//sys/clickhouse/defaults", client=client):
        defaults = update(defaults, get("//sys/clickhouse/defaults", client=client))

    require(operation_alias.startswith("*"), lambda: YtError("Operation alias should start with '*' character"))

    artifact_path = artifact_path or "//home/clickhouse-kolkhoz/" + operation_alias[1:]

    if abort_existing is None:
        abort_existing = False

    prev_operation = _resolve_alias(operation_alias, client=client)
    if operation_alias is not None:
        if prev_operation is not None:
            logger.info("Previous operation with alias %s is %s with state %s", operation_alias, prev_operation["id"], prev_operation["state"])
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

    op = run_operation(get_clickhouse_clique_spec_builder(instance_count,
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
