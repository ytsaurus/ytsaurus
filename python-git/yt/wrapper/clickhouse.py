from .operation_commands import TimeWatcher, process_operation_unsuccesful_finish_state
from .common import YtError, require, update
from .spec_builders import VanillaSpecBuilder
from .run_operation_commands import run_operation
from .cypress_commands import get, exists
from .ypath import FilePath
from .file_commands import smart_upload_file
from .yson import dumps

from tempfile import NamedTemporaryFile

DEFAULT_CPU_LIMIT = 8
DEFAULT_MEMORY_LIMIT = 15 * 2 ** 30
MEMORY_FOOTPRINT = 16 * 2 ** 30
DEFAULT_CYPRESS_BASE_CONFIG_PATH = "//sys/clickhouse/config"


def get_clickhouse_clique_spec_builder(instance_count,
                                       cypress_ytserver_clickhouse_path=None,
                                       host_ytserver_clickhouse_path=None,
                                       cypress_config_path=None,
                                       max_failed_job_count=None,
                                       cpu_limit=None,
                                       memory_limit=None,
                                       enable_monitoring=None,
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
    :param enable_monitoring: (only for development use) option that makes clickhouse bind monitoring port to 10042.
    :type enable_monitoring: bool
    :param spec: other spec options.
    :type spec: dict

    .. seealso::  :ref:`operation_parameters`.
    """

    if cpu_limit is None:
        cpu_limit = DEFAULT_CPU_LIMIT

    if memory_limit is None:
        memory_limit = DEFAULT_MEMORY_LIMIT

    if enable_monitoring is None:
        enable_monitoring = False

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

    if spec is None:
        spec = dict()
    if "annotations" not in spec:
        spec["annotations"] = dict()
    spec["annotations"]["is_clique"] = True
    if "expose" not in spec["annotations"]:
        spec["annotations"]["expose"] = True

    monitoring_port = "10142" if enable_monitoring else "$YT_PORT_1"

    spec_builder = \
        VanillaSpecBuilder() \
            .begin_task("clickhouse_servers") \
            .job_count(instance_count) \
            .file_paths(file_paths) \
            .command('{} --config config.yson --instance-id $YT_JOB_ID '
                     '--clique-id $YT_OPERATION_ID --rpc-port $YT_PORT_0 --monitoring-port {} '
                     '--tcp-port $YT_PORT_2 --http-port $YT_PORT_3'
                     .format(executable_path, monitoring_port)) \
            .memory_limit(memory_limit + MEMORY_FOOTPRINT) \
            .cpu_limit(cpu_limit) \
            .port_count(4) \
            .end_task() \
            .max_failed_job_count(max_failed_job_count) \
            .spec(spec)

    return spec_builder


def prepare_clickhouse_config(cypress_base_config_path=None, clickhouse_config=None, cpu_limit=None, memory_limit=None,
                              enable_query_log=None, client=None):
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

    if cpu_limit is None:
        cpu_limit = DEFAULT_CPU_LIMIT

    if memory_limit is None:
        memory_limit = DEFAULT_MEMORY_LIMIT

    if cypress_base_config_path is None:
        cypress_base_config_path = DEFAULT_CYPRESS_BASE_CONFIG_PATH

    if clickhouse_config is None:
        clickhouse_config = {}

    require(cpu_limit is not None, lambda: YtError("Cpu limit should be set to prepare the ClickHouse config"))
    require(memory_limit is not None, lambda: YtError("Memory limit should be set to prepare the ClickHouse config"))

    clickhouse_config["engine"] = clickhouse_config.get("engine", {})
    clickhouse_config["engine"]["settings"] = clickhouse_config["engine"].get("settings", {})
    clickhouse_config["engine"]["settings"]["max_threads"] = cpu_limit
    clickhouse_config["engine"]["settings"]["max_distributed_connections"] = cpu_limit

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
                            enable_monitoring=None,
                            enable_query_log=None,
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
    :param enable_monitoring: (only for development use) option that makes clickhouse bind monitoring port to 10042.
    :type enable_monitoring: bool
    :param enable_query_log: enable clickhouse query log.
    :type enable_query_log: bool
    .. seealso::  :ref:`operation_parameters`.
    """

    cypress_config_path = prepare_clickhouse_config(cypress_base_config_path=cypress_base_config_path,
                                                    clickhouse_config=clickhouse_config,
                                                    cpu_limit=cpu_limit,
                                                    memory_limit=memory_limit,
                                                    enable_query_log=enable_query_log,
                                                    client=client)

    op = run_operation(get_clickhouse_clique_spec_builder(instance_count,
                                                          cypress_config_path=cypress_config_path,
                                                          cpu_limit=cpu_limit,
                                                          memory_limit=memory_limit,
                                                          enable_monitoring=enable_monitoring,
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
