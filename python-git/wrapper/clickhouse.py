from .operation_commands import TimeWatcher, process_operation_unsuccesful_finish_state
from .common import YtError, require, update
from .spec_builders import VanillaSpecBuilder
from .run_operation_commands import run_operation
from .cypress_commands import get, exists
from .transaction_commands import _make_transactional_request
from .operation_commands import get_operation_url, abort_operation
from .http_helpers import get_proxy_url
from .ypath import FilePath
from .file_commands import smart_upload_file
from .config import get_config
from .yson import dumps, to_yson_type

import yt.logger as logger

from yt.packages.six import iteritems

from tempfile import NamedTemporaryFile
from inspect import getargspec

import json

CYPRESS_DEFAULTS_PATH = "//sys/clickhouse/defaults"
BUNDLED_DEFAULTS = {
    "memory_footprint": 16 * 1000**3,
    "memory_limit": 15 * 1000**3,
    "cypress_base_config_path": "//sys/clickhouse/config",
    "cpu_limit": 8,
    "enable_monitoring": False,
    "clickhouse_config": {},
    "max_failed_job_count": 10 * 1000,
    "use_exact_thread_count": True,
}


def _get_kwargs_names(fn):
    argspec = getargspec(fn)
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


def _determine_cluster(client=None):
    proxy_url = get_proxy_url(required=False, client=client)
    default_suffix = get_config(client)["proxy"]["default_suffix"]
    if proxy_url is not None and proxy_url.endswith(default_suffix):
        return proxy_url[:-len(default_suffix)]
    return None


def _format_url(url):
    return to_yson_type(url, attributes={"_type_tag": "url"})


def _build_description(cypress_ytserver_clickhouse_path=None, operation_alias=None, prev_operation_id=None, enable_monitoring=None, client=None):
    # Inherit all custom attributes from the ytserver-clickhouse.
    # TODO(max42): YT-11099.
    description = {}
    if cypress_ytserver_clickhouse_path is not None:
        attr_keys = get(cypress_ytserver_clickhouse_path + "/@user_attribute_keys", client=client)
        description = update(description, get(cypress_ytserver_clickhouse_path + "/@", attributes=attr_keys, client=client))

    # Put information about previous incarnation of the operation by the given alias (if any).
    if prev_operation_id is not None:
        description["previous_operation_id"] = prev_operation_id
        description["previous_operation_url"] = _format_url(get_operation_url(prev_operation_id, client=client))

    cluster = _determine_cluster(client=client)
    
    # Put link to yql query. It is currently possible to add it only when alias is specified, otherwise we do not have access to operation id.
    # TODO(max42): YT-11115.
    if cluster is not None and operation_alias is not None:
        description["yql_url"] = _format_url(
            "https://yql.yandex-team.ru/?query=use%20chyt.{}/{}%3B%0A%0Aselect%201%3B&query_type=CLICKHOUSE"
                .format(cluster, operation_alias[1:]))
       
    # Put link to monitoring.
    if cluster is not None and operation_alias is not None and enable_monitoring:
        description["monitoring_url"] = _format_url(
            "https://solomon.yandex-team.ru/?project=yt&cluster={}&service=yt_clickhouse&operation_alias={}"
                .format(cluster, operation_alias))

    return description

@_patch_defaults
def get_clickhouse_clique_spec_builder(instance_count,
                                       cypress_ytserver_clickhouse_path=None,
                                       host_ytserver_clickhouse_path=None,
                                       cypress_config_path=None,
                                       max_failed_job_count=None,
                                       cpu_limit=None,
                                       memory_limit=None,
                                       memory_footprint=None,
                                       enable_monitoring=None,
                                       cypress_geodata_path=None,
                                       core_dump_destination=None,
                                       description=None,
                                       operation_alias=None,
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

    require(cypress_config_path is not None,
            lambda: YtError("Cypress config.yson path should be specified; consider using "
                            "prepare_clickhouse_config helper"))
    file_paths = [FilePath(cypress_config_path, file_name="config.yson")]

    if cypress_ytserver_clickhouse_path is not None:
        executable_path = "./ytserver-clickhouse"
        file_paths.append(FilePath(cypress_ytserver_clickhouse_path, file_name="ytserver-clickhouse"))
    else:
        executable_path = host_ytserver_clickhouse_path

    if cypress_geodata_path is not None:
        file_paths.append(FilePath(cypress_geodata_path, file_name="geodata.tgz"))
        extract_geodata_command = "mkdir geodata ; tar xzf geodata.tgz -C geodata/ ;"
    else:
        extract_geodata_command = ""

    spec_base = {
        "annotations": {
            "is_clique": True,
            "expose": True,
        },
        "tasks": {
            "instances": {
                "user_job_memory_digest_lower_bound": 1.0
            }
        },
    }

    spec = update(spec_base, spec)

    monitoring_port = "10142" if enable_monitoring else "$YT_PORT_1"

    patch_config_command = "sed -s \"s/\$YT_JOB_INDEX/$YT_JOB_INDEX/g\" config.yson -i ;"

    run_clickhouse_command = "{} --config config.yson --instance-id $YT_JOB_ID " \
                             "--clique-id $YT_OPERATION_ID --rpc-port $YT_PORT_0 --monitoring-port {} " \
                             "--tcp-port $YT_PORT_2 --http-port $YT_PORT_3 ; ".format(executable_path, monitoring_port)

    if core_dump_destination is not None:
        copy_core_dumps_command = "exit_code=$? ;" \
                                  "if compgen -G 'core*' >/dev/null ; then " \
                                  "    echo 'Core dumps detected' >&2;" \
                                  "    mv core* {} ; " \
                                  "fi ;" \
                                  "exit $exit_code ;".format(core_dump_destination)
    else:
        copy_core_dumps_command = ""

    command = "\n".join([patch_config_command, extract_geodata_command, run_clickhouse_command, copy_core_dumps_command])

    spec_builder = \
        VanillaSpecBuilder() \
            .begin_task("instances") \
                .job_count(instance_count) \
                .file_paths(file_paths) \
                .command(command) \
                .memory_limit(memory_limit + memory_footprint) \
                .cpu_limit(cpu_limit) \
                .max_stderr_size(1024 * 1024 * 1024) \
                .port_count(4) \
            .end_task() \
            .max_failed_job_count(max_failed_job_count) \
            .description(description) \
            .max_stderr_count(150) \
            .alias(operation_alias) \
            .spec(spec)

    if "pool" not in spec_builder.build():
        logger.warning("It is discouraged to run clique in ephemeral pool "
                       "(which happens when pool is not specified explicitly)")

    return spec_builder


@_patch_defaults
def prepare_clickhouse_config(instance_count,
                              cypress_base_config_path=None,
                              clickhouse_config=None,
                              cpu_limit=None,
                              memory_limit=None,
                              memory_footprint=None,
                              use_exact_thread_count=None,
                              operation_alias=None,
                              client=None):
    """Merges a document pointed by `config_template_cypress_path`,  and `config` and uploads the
    result as a config.yson file suitable for specifying as a config file for clickhouse clique.

    :param cypress_base_config_path: path to a document that will be taken as a base config; if None, no base config is used
    :type cypress_base_config_path: str or None
    :param clickhouse_config: configuration patch to be applied onto the base config; if None, nothing happens
    :type clickhouse_config: dict or None
    :param enable_monitoring: (only for development use) option that makes clickhouse bind monitoring port to 10042.
    :type enable_monitoring: bool or None
    """

    require(cpu_limit is not None, lambda: YtError("Cpu limit should be set to prepare the ClickHouse config"))
    require(memory_limit is not None, lambda: YtError("Memory limit should be set to prepare the ClickHouse config"))

    thread_count = cpu_limit if use_exact_thread_count else 2 * max(cpu_limit, instance_count) + 1

    clickhouse_config_base = {
        "engine": {
            "settings": {
                "max_threads": thread_count,
                "max_distributed_connections": thread_count,
                "max_memory_usage_for_all_queries": memory_limit,
                "log_queries": 1,
            },
        },
        "memory_watchdog": {
            "memory_limit": memory_limit + memory_footprint,
        },
        "profile_manager": {
            "global_tags": {"operation_alias": operation_alias} if operation_alias is not None else {},
        },
    }

    clickhouse_config_cypress_base = get(cypress_base_config_path, client=client) if cypress_base_config_path != "" else None
    resulting_config = update(clickhouse_config_cypress_base, update(clickhouse_config_base, clickhouse_config))

    with NamedTemporaryFile() as temp:
        temp.write(dumps(resulting_config, yson_format="pretty"))
        temp.flush()
        result = smart_upload_file(temp.name, client=client)

    return str(result)


def start_clickhouse_clique(instance_count,
                            cypress_base_config_path=None,
                            cypress_ytserver_clickhouse_path=None,
                            host_ytserver_clickhouse_path=None,
                            clickhouse_config=None,
                            cpu_limit=None,
                            memory_limit=None,
                            memory_footprint=None,
                            enable_monitoring=None,
                            cypress_geodata_path=None,
                            description=None,
                            abort_existing=None,
                            operation_alias=None,
                            client=None,
                            **kwargs):
    """Starts a clickhouse clique consisting of a given number of instances.

    :param instance_count: number of instances (also the number of jobs in the underlying vanilla operation).
    :type instance_count: int
    :param clickhouse_config: patch to be applied to clickhouse config.
    :type clickhouse_config: dict or None
    :param cpu_limit: number of cores that will be available to each instance
    :type cpu_limit: int
    :param memory_limit: amount of memory that will be available to each instance
    :type memory_limit: int
    :param memory_footprint: amount of memory that goes to the YT runtime
    :type memory_footprint: int
    :param enable_monitoring: (only for development use) option that makes clickhouse bind monitoring port to 10042.
    :type enable_monitoring: bool
    :param description: YSON document which will be placed in cooresponding operation description.
    :type description: str or None
    :param abort_existing: should we abort existing operation with the given alias?
    :type abort_existing: bool or None
    :param cypress_geodata_path: path to archive with geodata in Cypress
    :type cypress_geodata_path str or None
    .. seealso::  :ref:`operation_parameters`.
    """

    defaults = get("//sys/clickhouse/defaults", client=client) if exists("//sys/clickhouse/defaults", client=client) else BUNDLED_DEFAULTS

    if abort_existing is None:
        abort_existing = False

    if abort_existing and operation_alias is None:
        logger.warning("Abort existing is meaningless without specifying operation alias")
        abort_existing = False

    prev_operation = _resolve_alias(operation_alias, client=client)
    if abort_existing:
        if prev_operation is not None and prev_operation["state"] == "running":
            logger.info("Aborting previous operation with alias " + operation_alias)
            abort_operation(prev_operation["id"], client=client)
        else:
            logger.info("There is no running operation with alias " + operation_alias)
    prev_operation_id = prev_operation["id"] if prev_operation is not None else None

    if cypress_ytserver_clickhouse_path is None and host_ytserver_clickhouse_path is None:
        cypress_ytserver_clickhouse_path = "//sys/clickhouse/bin/ytserver-clickhouse"
    require(cypress_ytserver_clickhouse_path is None or host_ytserver_clickhouse_path is None,
            lambda: YtError("Cypress ytserver-clickhouse binary path and host ytserver-clickhouse path "
                            "cannot be specified at the same time"))

    cypress_config_path = prepare_clickhouse_config(instance_count,
                                                    cypress_base_config_path=cypress_base_config_path,
                                                    clickhouse_config=clickhouse_config,
                                                    cpu_limit=cpu_limit,
                                                    memory_limit=memory_limit,
                                                    defaults=defaults,
                                                    operation_alias=operation_alias,
                                                    client=client)

    description = update(description, _build_description(cypress_ytserver_clickhouse_path=cypress_ytserver_clickhouse_path,
                                                         operation_alias=operation_alias,
                                                         prev_operation_id=prev_operation_id,
                                                         enable_monitoring=enable_monitoring,
                                                         client=client))

    op = run_operation(get_clickhouse_clique_spec_builder(instance_count,
                                                          cypress_config_path=cypress_config_path,
                                                          cpu_limit=cpu_limit,
                                                          memory_limit=memory_limit,
                                                          memory_footprint=memory_footprint,
                                                          enable_monitoring=enable_monitoring,
                                                          cypress_ytserver_clickhouse_path=cypress_ytserver_clickhouse_path,
                                                          host_ytserver_clickhouse_path=host_ytserver_clickhouse_path,
                                                          cypress_geodata_path=cypress_geodata_path,
                                                          operation_alias=operation_alias,
                                                          description=description,
                                                          defaults=defaults,
                                                          **kwargs),
                       client=client,
                       sync=False)

    for state in op.get_state_monitor(TimeWatcher(1.0, 1.0, 0.0)):
        if state.is_running() and \
                exists("//sys/clickhouse/cliques/{0}".format(op.id), client=client) and \
                get("//sys/clickhouse/cliques/{0}/@count".format(op.id), client=client) == instance_count:
            return op
        elif state.is_unsuccessfully_finished():
            process_operation_unsuccesful_finish_state(op, state)
        else:
            op.printer(state)
