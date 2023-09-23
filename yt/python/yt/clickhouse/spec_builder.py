from .defaults import patch_defaults

from yt.wrapper.spec_builders import VanillaSpecBuilder
from yt.wrapper.ypath import FilePath
from yt.wrapper.common import YtError, require, update

import yt.logger as logger

try:
    from yt.packages.six import itervalues
except ImportError:
    from six import itervalues


# NB: this method is used not only in CLI, but also in CHYT integration tests.
# Keep that in mind when changing it and do not forget to run both Python API tests
# and integration tests.
@patch_defaults
def get_clique_spec_builder(instance_count,
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
                            enable_monitoring=None,
                            cypress_geodata_path=None,
                            core_dump_destination=None,
                            description=None,
                            operation_alias=None,
                            memory_config=None,
                            enable_job_tables=None,
                            enable_log_tailer=None,
                            trampoline_log_file=None,
                            stderr_file=None,
                            max_instance_count=None,
                            spec=None,
                            client=None,
                            ytserver_readiness_timeout=None,
                            tvm_secret=None):
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

    require(cypress_config_paths is not None,
            lambda: YtError("At least cypress clickhouse server config.yson path should be specified as cypress_config_"
                            "paths dictionary; consider using prepare_cypress_configs helper"))

    require(instance_count <= max_instance_count,
            lambda: YtError("Requested instance count exceeds maximum allowed instance count: {} > {}; if you indeed "
                            "want to run clique of such size, consult with CHYT support in support "
                            "chat".format(instance_count, max_instance_count)))

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
        if host_bin_path is not None:
            return host_bin_path
        elif cypress_bin_path is not None:
            file_paths.append(FilePath(cypress_bin_path, file_name=bin_name))
            return "./" + bin_name
        else:
            return None

    ytserver_clickhouse_path = add_file(cypress_ytserver_clickhouse_path, host_ytserver_clickhouse_path,
                                        "ytserver-clickhouse")
    clickhouse_trampoline_path = add_file(cypress_clickhouse_trampoline_path, host_clickhouse_trampoline_path,
                                          "clickhouse-trampoline")
    ytserver_log_tailer_path = add_file(cypress_ytserver_log_tailer_path, host_ytserver_log_tailer_path,
                                        "ytserver-log-tailer")

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

    if stderr_file:
        args += ["--stderr-file", stderr_file]

    if ytserver_readiness_timeout is not None:
        args += ["--readiness-timeout", str(ytserver_readiness_timeout)]

    trampoline_command = " ".join(args)

    spec_builder = \
        VanillaSpecBuilder() \
            .begin_task("instances") \
                .job_count(instance_count) \
                .file_paths(file_paths) \
                .command(trampoline_command) \
                .memory_limit(memory_config["memory_limit"] + memory_config["log_tailer"]) \
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
            .spec(spec)  # noqa

    if tvm_secret is not None:
        spec_builder = spec_builder.secure_vault({"TVM_SECRET": tvm_secret})

    if "pool" not in spec_builder.build(client=client):
        logger.warning("It is discouraged to run clique in ephemeral pool "
                       "(which happens when pool is not specified explicitly)")

    return spec_builder
