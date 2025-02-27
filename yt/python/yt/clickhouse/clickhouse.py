from .defaults import patch_defaults
from .spec_builder import get_clique_spec_builder
from .log_tailer import prepare_log_tailer_tables
from .compatibility import validate_ytserver_clickhouse_version, LAUNCHER_VERSION
from .helpers import get_alias_from_env_or_raise

from yt.wrapper.operation_commands import TimeWatcher, process_operation_unsuccessful_finish_state
from yt.wrapper.common import YtError, require, update, update_inplace
from yt.wrapper.run_operation_commands import run_operation
from yt.wrapper.cypress_commands import get, exists, copy, create, list
from yt.wrapper.operation_commands import get_operation_url, abort_operation, get_operation
from yt.wrapper.http_helpers import get_cluster_name
from yt.wrapper.file_commands import smart_upload_file
from yt.wrapper.config import get_config
from yt.wrapper.yson import dumps, to_yson_type

import yt.logger as logger

try:
    from yt.packages.six.moves import xrange
except ImportError:
    from six.moves import xrange

from copy import deepcopy
from tempfile import NamedTemporaryFile

import os
import random


def _resolve_alias(operation_alias, client=None):
    if operation_alias is None:
        return None
    try:
        return get_operation(
            operation_alias=operation_alias,
            include_runtime=True,
            attributes=["id", "state"],
            client=client)
    except:  # noqa
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
            "https://yql.yandex-team.ru/?query=use%20chyt.{}/{}%3B%0A%0Aselect%201%3B&query_type=CLICKHOUSE".format(
                cluster, operation_alias[1:]))

    # Put link to monitoring.
    if cluster is not None and operation_alias is not None and enable_monitoring:
        solomon_dashboard = "chyt_v2"
        solomon_service = "clickhouse"

        description["solomon_root_url"] = _format_url(
            "https://solomon.yandex-team.ru/?project=yt&cluster={}&service={}&operation_alias={}".format(
                cluster, solomon_service, operation_alias[1:]))
        description["solomon_dashboard_url"] = _format_url(
            "https://solomon.yandex-team.ru/?project=yt&cluster={}&service={}&cookie=Aggr&dashboard={}&l.operation_alias={}".format(
                cluster, solomon_service, solomon_dashboard, operation_alias[1:]))

    return description


@patch_defaults
def prepare_configs(instance_count,
                    cypress_base_config_path=None,
                    cypress_log_tailer_config_path=None,
                    clickhouse_config=None,
                    cpu_limit=None,
                    memory_config=None,
                    operation_alias=None,
                    client=None):
    require(cpu_limit is not None, lambda: YtError("Cpu limit should be set to prepare the ClickHouse config"))
    require(memory_config is not None, lambda: YtError("Memory config should be set to prepare the ClickHouse config"))

    clickhouse_config_base = {
        # COMPAT(max42): rename into "clickhouse".
        "engine": {
            "settings": {
                "max_threads": cpu_limit,
                # COMPAT(max42): fresh CH does not recognize this option.
                "max_memory_usage_for_all_queries": memory_config["clickhouse"],
                "log_queries": 1,
                "queue_max_wait_ms": 30 * 1000,
            },
            "max_server_memory_usage": memory_config["max_server_memory_usage"]
        },
        "profile_manager": {
            "global_tags": {"operation_alias": operation_alias[1:]} if operation_alias is not None else {},
        },
        "cluster_connection": {
            "block_cache": {
                "uncompressed_data": {
                    "capacity": memory_config["uncompressed_block_cache"],
                },
            },
        },
        # COMPAT(max42): remove four fields below.
        "discovery": {
            "directory": "//sys/clickhouse/cliques",
        },
        "memory_watchdog": {
            "memory_limit": memory_config["memory_limit"],
        },
        "worker_thread_count": cpu_limit,
        "cpu_limit": cpu_limit,
        "yt": {
            "worker_thread_count": cpu_limit,
            "cpu_limit": cpu_limit,
            "memory_watchdog": {
                "memory_limit": memory_config["memory_limit"],
            },
        },
        "launcher": {
            "version": LAUNCHER_VERSION,
        },
        "memory": memory_config,
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

    log_tailer_version = get(cypress_ytserver_log_tailer_path + "/@yt_version", client=client) if cypress_ytserver_log_tailer_path else ""

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
    def is_active(instance):
        if not instance.attributes["locks"]:
            return False
        for lock in instance.attributes["locks"]:
            if lock["child_key"] and lock["child_key"] == "lock":
                return True
        return False

    for state in op.get_state_monitor(TimeWatcher(1.0, 1.0, 0.0)):
        if state.is_running() and exists("//sys/clickhouse/cliques/{0}".format(op.id), client=client):
            instances = list("//sys/clickhouse/cliques/{0}".format(op.id), attributes=["locks"], client=client)
            if sum(1 for instance in instances if is_active(instance)) == instance_count:
                logger.info("Clique started and ready for serving under alias %s", operation_alias)
                return op
        elif state.is_unsuccessfully_finished():
            process_operation_unsuccessful_finish_state(op, op.get_error(state))
        else:
            op.printer(state)


@patch_defaults
def process_memory_config(memory_config=None):
    if memory_config is None:
        raise YtError("Missing memory config; CHYT defaults for the cluster seem to be obsolete")
    allowed_keys = {"reader", "uncompressed_block_cache", "compressed_block_cache", "chunk_meta_cache", "clickhouse",
                    "footprint", "clickhouse_watermark", "watchdog_oom_watermark", "log_tailer"}
    for key in allowed_keys:
        if key not in memory_config:
            raise YtError("Missing memory config key {}; CHYT defaults for the cluster seem to be obsolete".format(key))
    max_server_memory_usage_keys = {
        "reader", "uncompressed_block_cache", "compressed_block_cache", "clickhouse", "footprint", "chunk_meta_cache"
    }
    memory_config["max_server_memory_usage"] = sum(memory_config[key] for key in max_server_memory_usage_keys)
    memory_config["memory_limit"] = \
        memory_config["max_server_memory_usage"] + memory_config["clickhouse_watermark"]

    return memory_config


@patch_defaults
def validate_dominant_resource(memory_config=None, cpu_limit=None, desired_memory_per_core=None):
    memory_per_core = memory_config["memory_limit"] / cpu_limit
    if memory_per_core > desired_memory_per_core:
        logger.warning("Memory per CPU core is higher than recommended: actual = {} > desired = {}; "
                       "consider increasing CPU limit".format(memory_per_core, desired_memory_per_core))


def start_clique(instance_count,
                 alias=None,
                 cypress_base_config_path=None,
                 cypress_ytserver_clickhouse_path=None,
                 cypress_clickhouse_trampoline_path=None,
                 cypress_ytserver_log_tailer_path=None,
                 host_ytserver_clickhouse_path=None,
                 host_clickhouse_trampoline_path=None,
                 host_ytserver_log_tailer_path=None,
                 clickhouse_config=None,
                 cpu_limit=None,
                 memory_config=None,
                 enable_monitoring=None,
                 cypress_geodata_path=None,
                 description=None,
                 abort_existing=None,
                 dump_tables=None,
                 spec=None,
                 cypress_log_tailer_config_path=None,
                 enable_job_tables=None,
                 artifact_path=None,
                 client=None,
                 wait_for_instances=None,
                 skip_version_compatibility_validation=None,
                 **kwargs):
    """Starts a CHYT clique consisting of a given number of instances.

    :param alias alias for the underlying YT operation
    :type alias: str
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
    :param memory_config: amount of per-category memory that will be available to each instance; should be dict,
    following keys are possible: 'reader', 'uncompressed_block_cache', 'clickhouse', 'footprint',
    'clickhouse_watermark', 'watchdog_oom_watermark', 'log_tailer'.
    :type memory_config: dict or None
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
    :param skip_version_compatibility_validation: skip version compatibility validation; do not use this without consulting
    with CHYT team.
    :type skip_version_compatibility_validation bool or None
    .. seealso::  :ref:`operation_parameters`.
    """

    alias = alias or get_alias_from_env_or_raise()

    if exists("//sys/clickhouse/defaults", client=client):
        defaults = get("//sys/clickhouse/defaults", client=client)
    else:
        raise YtError("CHYT is not set up on cluster: //sys/clickhouse/defaults not found")

    require(alias.startswith("*"), lambda: YtError("Operation alias should start with '*' character"))

    artifact_path = artifact_path or "//home/clickhouse-kolkhoz/" + alias[1:]

    if abort_existing is None:
        abort_existing = False

    skip_version_compatibility_validation = \
        skip_version_compatibility_validation or defaults.get("skip_version_compatibility_validation", False)

    prev_operation = _resolve_alias(alias, client=client)
    if alias is not None:
        if prev_operation is not None:
            logger.info("Previous operation with alias %s is %s with state %s",
                        alias, prev_operation["id"], prev_operation["state"])
            if not abort_existing:
                raise YtError("There is already an operation with alias {}; "
                              "abort it or specify --abort-existing command-line flag".format(alias))
        else:
            logger.info("There was no operation with alias %s before", alias)

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

    description = update(description, _build_description(cypress_ytserver_clickhouse_path=cypress_ytserver_clickhouse_path,
                                                         cypress_ytserver_log_tailer_path=cypress_ytserver_log_tailer_path,
                                                         cypress_clickhouse_trampoline_path=cypress_clickhouse_trampoline_path,
                                                         operation_alias=alias,
                                                         prev_operation_id=prev_operation_id,
                                                         enable_monitoring=enable_monitoring,
                                                         defaults=defaults,
                                                         client=client))

    if not skip_version_compatibility_validation:
        validate_ytserver_clickhouse_version(description.get("ytserver-clickhouse"))
    else:
        logger.warning("Version validation has been skipped")

    if wait_for_instances is None:
        wait_for_instances = True

    if clickhouse_config is not None and "memory" in clickhouse_config:
        memory_config = update(memory_config, clickhouse_config["memory"])

    memory_config = process_memory_config(memory_config=memory_config, defaults=defaults)
    validate_dominant_resource(
        memory_config=memory_config,
        cpu_limit=cpu_limit,
        desired_memory_per_core=defaults.get("desired_memory_per_core", 5 * 1024**3),
        defaults=defaults)

    configs = prepare_configs(instance_count,
                              cypress_base_config_path=cypress_base_config_path,
                              cypress_log_tailer_config_path=cypress_log_tailer_config_path,
                              clickhouse_config=clickhouse_config,
                              cpu_limit=cpu_limit,
                              memory_config=memory_config,
                              defaults=defaults,
                              operation_alias=alias,
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

    if abort_existing:
        if prev_operation is not None and not prev_operation["state"].endswith("ed"):
            logger.info("Aborting previous operation with alias %s", alias)
            abort_operation(prev_operation["id"], client=client)
        else:
            logger.info("There is no running operation with alias %s; not aborting anything", alias)

    op = run_operation(get_clique_spec_builder(instance_count,
                                               artifact_path=artifact_path,
                                               cypress_config_paths=cypress_config_paths,
                                               cpu_limit=cpu_limit,
                                               enable_monitoring=enable_monitoring,
                                               cypress_ytserver_clickhouse_path=cypress_ytserver_clickhouse_path,
                                               cypress_clickhouse_trampoline_path=cypress_clickhouse_trampoline_path,
                                               cypress_ytserver_log_tailer_path=cypress_ytserver_log_tailer_path,
                                               host_ytserver_clickhouse_path=host_ytserver_clickhouse_path,
                                               host_clickhouse_trampoline_path=host_clickhouse_trampoline_path,
                                               host_ytserver_log_tailer_path=host_ytserver_log_tailer_path,
                                               cypress_geodata_path=cypress_geodata_path,
                                               operation_alias=alias,
                                               description=description,
                                               memory_config=memory_config,
                                               spec=spec,
                                               enable_job_tables=enable_job_tables,
                                               enable_log_tailer=True,
                                               defaults=defaults,
                                               client=client,
                                               **kwargs),
                       client=client,
                       sync=False)

    if wait_for_instances:
        do_wait_for_instances(op, instance_count, alias, client=client)

    return op
