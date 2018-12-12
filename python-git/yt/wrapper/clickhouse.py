from .operation_commands import TimeWatcher, process_operation_unsuccesful_finish_state
from .common import YtError, require, update
from .spec_builders import VanillaSpecBuilder
from .run_operation_commands import run_operation
from .cypress_commands import get, exists
from .ypath import YPath
from .file_commands import smart_upload_file
from .yson import dumps

from tempfile import NamedTemporaryFile
from os import unlink

def get_clickhouse_clique_spec_builder(instance_count,
                                       cypress_ytserver_clickhouse_path=None,
                                       host_ytserver_clickhouse_path=None,
                                       cypress_config_path=None,
                                       max_failed_job_count=None,
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
    :param spec: other spec options.
    :type spec: dict

    .. seealso::  :ref:`operation_parameters`.
    """

    require(cypress_config_path is not None,
            lambda: YtError("Cypress config.yson path should be specified; consider using "
                            "prepare_clickhouse_config helper"))
    file_paths = [YPath(cypress_config_path, attributes={"file_name": "config.yson"})]
    if cypress_ytserver_clickhouse_path is None and host_ytserver_clickhouse_path is None:
        cypress_ytserver_clickhouse_path = "//sys/clickhouse/bin/ytserver-clickhouse"
    require(cypress_ytserver_clickhouse_path is None or host_ytserver_clickhouse_path is None,
            lambda: YtError("Cypress ytserver-clickhouse binary path and host ytserver-clickhouse path "
                            "cannot be specified at the same time"))

    if cypress_ytserver_clickhouse_path is not None:
        executable_path = "./ytserver-clickhouse"
        file_paths.append(YPath(cypress_ytserver_clickhouse_path, attributes={"file_name": "ytserver-clickhouse"}))
    else:
        executable_path = host_ytserver_clickhouse_path

    if spec is None:
        spec = dict()
    if "annotations" not in spec:
        spec["annotations"] = dict()
    if "expose_to_yql" not in spec["annotations"]:
        spec["annotations"]["expose_to_yql"] = True

    spec_builder = \
        VanillaSpecBuilder() \
            .begin_task("clickhouse_servers") \
                .job_count(instance_count) \
                .file_paths(file_paths) \
                .command('{} --config config.yson --instance-id $YT_JOB_ID '
                         '--clique-id $YT_OPERATION_ID --rpc-port $YT_PORT_0 --monitoring-port $YT_PORT_1 '
                         '--tcp-port $YT_PORT_2 --http-port $YT_PORT_3'
                         .format(executable_path)) \
                .memory_limit(10 * 2**30) \
                .port_count(4) \
            .end_task() \
            .max_failed_job_count(max_failed_job_count) \
            .spec(spec)

    return spec_builder

def prepare_clickhouse_config(cypress_base_config_path=None, clickhouse_config=None, client=None):
    """Merges a document pointed by `config_template_cypress_path` and `config` and uploads the
    result as a config.yson file suitable for specifying as a config file for clickhouse clique.

    :param cypress_base_config_path: path to a document that will be taken as a base config; if None, no base config is used
    :type cypress_base_config_path: str or None
    :param clickhouse_config: configuration patch to be applied onto the base config; if None, nothing happens
    :type clickhouse_config: dict or None
    :return: path to the resulting file in Cypress.
    """

    if clickhouse_config is None:
        clickhouse_config = {}
    base_config = get(cypress_base_config_path, client=client) if cypress_base_config_path not in [None, ""] else {}
    resulting_config = update(base_config, clickhouse_config)
    temp = NamedTemporaryFile(delete=False)
    temp.write(dumps(resulting_config, yson_format="pretty"))
    temp.close()

    result = smart_upload_file(temp.name)

    unlink(temp.name)

    return str(result)

def start_clickhouse_clique(instance_count, cypress_base_config_path="//sys/clickhouse/config", clickhouse_config=None, client=None, **kwargs):
    """Starts a clickhouse clique consisting of a given number of instances.

    :param instance_count: number of instances (also the number of jobs in the underlying vanilla operation).
    :type instance_count: int
    :param clickhouse_config: patch to be applied to clickhouse config.
    :type clickhouse_config: dict or None

    .. seealso::  :ref:`operation_parameters`.
    """

    cypress_config_path = prepare_clickhouse_config(cypress_base_config_path=cypress_base_config_path,
                                                    clickhouse_config=clickhouse_config,
                                                    client=client)

    op = run_operation(get_clickhouse_clique_spec_builder(instance_count,
                                                          cypress_config_path=cypress_config_path,
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

