from .operation_commands import TimeWatcher, process_operation_unsuccesful_finish_state
from .common import YtError, require, update
from .spec_builders import VanillaSpecBuilder
from .run_operation_commands import run_operation
from .cypress_commands import get, exists
from .ypath import FilePath
from .file_commands import smart_upload_file
from .yson import dumps

import yt.logger as logger

from yt.packages.six import iteritems

from tempfile import NamedTemporaryFile

CYPRESS_DEFAULTS_PATH = "//sys/clickhouse/defaults"
BUNDLED_DEFAULTS = {
    "memory_footprint": 16 * 1000**3,
    "memory_limit": 15 * 1000**3,
    "cypress_base_config_path": "//sys/clickhouse/config",
    "cpu_limit": 8,
    "enable_monitoring": False,
    "clickhouse_config": {},
    "use_exact_thread_count": True,
}


def get_kwargs_names(fn):
    varnames = fn.func_code.co_varnames[:fn.func_code.co_argcount]
    kwargs_len = len(fn.func_defaults)
    varnames = varnames[-kwargs_len:]
    return varnames


def patch_defaults(fn):
    kwargs_names = get_kwargs_names(fn)

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

    return wrapped_fn


@patch_defaults
def get_clickhouse_clique_spec_builder(instance_count,
                                       cypress_ytserver_clickhouse_path=None,
                                       host_ytserver_clickhouse_path=None,
                                       cypress_config_path=None,
                                       max_failed_job_count=None,
                                       cpu_limit=None,
                                       memory_limit=None,
                                       memory_footprint=None,
                                       enable_monitoring=None,
                                       set_container_cpu_limit=None,
                                       cypress_geodata_path=None,
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
    if cypress_ytserver_clickhouse_path is None and host_ytserver_clickhouse_path is None:
        cypress_ytserver_clickhouse_path = "//sys/clickhouse/bin/ytserver-clickhouse"
    require(cypress_ytserver_clickhouse_path is None or host_ytserver_clickhouse_path is None,
            lambda: YtError("Cypress ytserver-clickhouse binary path and host ytserver-clickhouse path "
                            "cannot be specified at the same time"))

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

    if spec is None:
        spec = dict()
    if "annotations" not in spec:
        spec["annotations"] = dict()
    spec["annotations"]["is_clique"] = True
    if "expose" not in spec["annotations"]:
        spec["annotations"]["expose"] = True

    monitoring_port = "10142" if enable_monitoring else "$YT_PORT_1"

    run_clickhouse_command = "{} --config config.yson --instance-id $YT_JOB_ID "\
                             "--clique-id $YT_OPERATION_ID --rpc-port $YT_PORT_0 --monitoring-port {} "\
                             "--tcp-port $YT_PORT_2 --http-port $YT_PORT_3 ;".format(executable_path, monitoring_port)
    command = "{}\n{}".format(extract_geodata_command, run_clickhouse_command)

    spec_builder = \
        VanillaSpecBuilder() \
            .begin_task("clickhouse_servers") \
                .job_count(instance_count) \
                .file_paths(file_paths) \
                .command(command) \
                .memory_limit(memory_limit + memory_footprint) \
                .cpu_limit(cpu_limit) \
                .port_count(4) \
                .set_container_cpu_limit(set_container_cpu_limit) \
            .end_task() \
            .max_failed_job_count(max_failed_job_count) \
            .spec(spec)

    return spec_builder


@patch_defaults
def prepare_clickhouse_config(instance_count,
                              cypress_base_config_path=None,
                              clickhouse_config=None,
                              cpu_limit=None,
                              memory_limit=None,
                              enable_query_log=None,
                              use_exact_thread_count=None,
                              client=None):
    """Merges a document pointed by `config_template_cypress_path` and `config` and uploads the
    result as a config.yson file suitable for specifying as a config file for clickhouse clique.

    :param cypress_base_config_path: path to a document that will be taken as a base config; if None, no base config is used
    :type cypress_base_config_path: str or None
    :param clickhouse_config: configuration patch to be applied onto the base config; if None, nothing happens
    :type clickhouse_config: dict or None
    :param enable_monitoring: (only for development use) option that makes clickhouse bind monitoring port to 10042.
    :type enable_monitoring: bool or None
    :param enable_query_log: enable clickhouse query log.
    :type enable_query_log: bool or None
    """

    require(cpu_limit is not None, lambda: YtError("Cpu limit should be set to prepare the ClickHouse config"))
    require(memory_limit is not None, lambda: YtError("Memory limit should be set to prepare the ClickHouse config"))

    thread_count = cpu_limit if use_exact_thread_count else 2 * max(cpu_limit, instance_count) + 1

    clickhouse_config["engine"] = clickhouse_config.get("engine", {})
    clickhouse_config["engine"]["settings"] = clickhouse_config["engine"].get("settings", {})
    clickhouse_config["engine"]["settings"]["max_threads"] = thread_count
    clickhouse_config["engine"]["settings"]["max_distributed_connections"] = thread_count

    clickhouse_config["engine"] = clickhouse_config.get("engine", {})
    clickhouse_config["engine"]["settings"] = clickhouse_config["engine"].get("settings", {})
    clickhouse_config["engine"]["settings"]["max_memory_usage_for_all_queries"] = memory_limit

    if enable_query_log:
        clickhouse_config["engine"]["settings"]["log_queries"] = 1

    base_config = get(cypress_base_config_path, client=client) if cypress_base_config_path != "" else {}
    resulting_config = update(base_config, clickhouse_config)

    with NamedTemporaryFile() as temp:
        temp.write(dumps(resulting_config, yson_format="pretty"))
        temp.flush()
        result = smart_upload_file(temp.name, client=client)

    return str(result)


def start_clickhouse_clique(instance_count,
                            cypress_base_config_path=None,
                            clickhouse_config=None,
                            cpu_limit=None,
                            memory_limit=None,
                            memory_footprint=None,
                            enable_monitoring=None,
                            enable_query_log=None,
                            cypress_geodata_path=None,
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
    :param enable_query_log: enable clickhouse query log.
    :type enable_query_log: bool
    :param cypress_geodata_path: path to archive with geodata in Cypress
    :type cypress_geodata_path str or None
    .. seealso::  :ref:`operation_parameters`.
    """

    defaults = get("//sys/clickhouse/defaults", client=client) if exists("//sys/clickhouse/defaults", client=client) else BUNDLED_DEFAULTS

    cypress_config_path = prepare_clickhouse_config(instance_count,
                                                    cypress_base_config_path=cypress_base_config_path,
                                                    clickhouse_config=clickhouse_config,
                                                    cpu_limit=cpu_limit,
                                                    memory_limit=memory_limit,
                                                    enable_query_log=enable_query_log,
                                                    defaults=defaults,
                                                    client=client)

    op = run_operation(get_clickhouse_clique_spec_builder(instance_count,
                                                          cypress_config_path=cypress_config_path,
                                                          cpu_limit=cpu_limit,
                                                          memory_limit=memory_limit,
                                                          memory_footprint=memory_footprint,
                                                          enable_monitoring=enable_monitoring,
                                                          cypress_geodata_path=cypress_geodata_path,
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
