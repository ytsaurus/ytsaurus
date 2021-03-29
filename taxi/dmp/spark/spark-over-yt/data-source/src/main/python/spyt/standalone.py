import copy
import logging
import os
import re
import subprocess

from yt.wrapper.common import update_inplace, update
from yt.wrapper.cypress_commands import exists
from yt.wrapper.http_helpers import get_token, get_user_name, get_proxy_url
from yt.wrapper.operation_commands import TimeWatcher, process_operation_unsuccesful_finish_state
from yt.wrapper.run_operation_commands import run_operation
from yt.wrapper.spec_builders import VanillaSpecBuilder

from .conf import read_remote_conf, validate_cluster_version, spyt_jar_path, spyt_python_path, \
    validate_spyt_version, validate_versions_compatibility, latest_compatible_spyt_version, \
    latest_cluster_version, update_config_inplace, validate_custom_params, validate_mtn_config, \
    latest_ytserver_proxy_path, ytserver_proxy_attributes, read_global_conf
from .utils import get_spark_master, base_spark_conf, SparkDiscovery, SparkCluster
from .enabler import SpytEnablers

logger = logging.getLogger(__name__)


class SparkDefaultArguments(object):
    SPARK_WORKER_TMPFS_LIMIT = "150G"
    SPARK_MASTER_MEMORY_LIMIT = "4G"
    SPARK_HISTORY_SERVER_MEMORY_LIMIT = "8G"
    DYNAMIC_CONFIG_PATH = "//sys/spark/bin/releases/spark-launch-conf"
    SPARK_WORKER_TIMEOUT = "5m"
    SPARK_WORKER_CORES_OVERHEAD = 0
    SPARK_WORKER_CORES_BYOP_OVERHEAD = 0

    @staticmethod
    def get_params():
        return {
            "operation_spec": {
                "annotations": {"is_spark": True},
                "max_failed_job_count": 5,
                "max_stderr_count": 150,
                "job_cpu_monitor": {
                    "enable_cpu_reclaim": False
                }
            }
        }


def _add_conf(spark_conf, spark_args):
    if spark_conf:
        for k, v in spark_conf.items():
            spark_args.append("--conf")
            spark_args.append("{}={}".format(k, v))


def _add_master(discovery, spark_args, rest, client=None):
    spark_args.append("--master")
    spark_args.append(get_spark_master(discovery, rest=False, yt_client=client))
    _add_conf({
        "spark.rest.master": get_spark_master(discovery, rest=True, yt_client=client)
    }, spark_args)


def _add_base_spark_conf(client, discovery, spark_args):
    _add_conf(base_spark_conf(client, discovery), spark_args)


def _add_job_args(job_args, spark_args):
    if job_args:
        for k, v in job_args.items():
            spark_args.append("--{}".format(k))
            spark_args.append(v)


def _create_spark_env(client, spark_home):
    spark_env = os.environ.copy()
    yt_token = get_token(client=client)
    yt_user = get_user_name(client=client)
    yt_proxy = get_proxy_url(client=client)
    spark_env["SPARK_USER"] = yt_user
    spark_env["SPARK_YT_TOKEN"] = yt_token
    spark_env["SPARK_YT_PROXY"] = yt_proxy
    if spark_home:
        spark_env["SPARK_HOME"] = spark_home
    return spark_env


def _parse_memory(memory_str):
    units = {"gb": 1024 * 1024 * 1024, "mb": 1024 * 1024, "kb": 1024, "bb": 1, "b": 1}
    if memory_str is None:
        return None
    m = re.match(r"(\d+)(.*)", memory_str)
    value = int(m.group(1))
    unit = m.group(2).lower().strip()
    if len(unit) <= 1:
        unit = unit + "b"
    return value * units[unit]


def _wait_master_start(op, spark_discovery, client):
    operation_path = spark_discovery.operation().join(op.id)
    for state in op.get_state_monitor(TimeWatcher(1.0, 1.0, 0.0)):
        if state.is_running() and exists(operation_path, client=client):
            return op
        elif state.is_unsuccessfully_finished():
            process_operation_unsuccesful_finish_state(op, state)
        else:
            op.printer(state)


def _jmx_opts(port):
    return "-Dcom.sun.management.jmxremote " \
           "-Dcom.sun.management.jmxremote.port={} " \
           "-Dcom.sun.management.jmxremote.authenticate=false " \
           "-Dcom.sun.management.jmxremote.ssl=false".format(port)


def submit(discovery_path, spark_home, deploy_mode, spark_conf, job_class, jar_path, job_args, client=None):
    spark_args = ["--deploy-mode", deploy_mode]

    _add_conf(spark_conf, spark_args)

    spark_args.append("--class")
    spark_args.append(job_class)
    spark_args.append(jar_path)

    _add_job_args(job_args, spark_args)

    raw_submit(discovery_path=discovery_path,
               spark_home=spark_home,
               spark_args=spark_args,
               client=client)


def submit_python(discovery_path, spark_home, deploy_mode, spark_conf, main_py_path, py_files, job_args, client=None):
    spark_args = ["--deploy-mode", deploy_mode]

    _add_conf(spark_conf, spark_args)

    spark_args.append("--py-files")
    spark_args.append(py_files)
    spark_args.append(main_py_path)

    _add_job_args(job_args, spark_args)

    raw_submit(discovery_path=discovery_path,
               spark_home=spark_home,
               spark_args=spark_args,
               client=client)


def raw_submit(discovery_path, spark_home, spark_args, spyt_version=None, client=None, spark_id=None):
    spark_submit_path = "{}/bin/spark-submit".format(spark_home)
    spark_base_args = [spark_submit_path]
    discovery = SparkDiscovery(discovery_path=discovery_path, spark_id=spark_id)
    _add_master(discovery, spark_base_args, rest=True, client=client)
    _add_base_spark_conf(client, discovery, spark_base_args)
    _add_spyt_deps(spyt_version, spark_base_args, discovery, client)
    spark_env = _create_spark_env(client, spark_home)

    # replace stdin to avoid https://bugs.openjdk.java.net/browse/JDK-8211842
    return subprocess.call(spark_base_args + spark_args, env=spark_env, stdin=subprocess.PIPE)


def _add_spyt_deps(spyt_version, spark_args, discovery, client):
    spark_cluster_version = SparkDiscovery.get(discovery.spark_cluster_version(), client=client)
    if spyt_version is not None:
        validate_spyt_version(spyt_version, client=client)
        validate_versions_compatibility(spyt_version, spark_cluster_version)
    else:
        spyt_version = latest_compatible_spyt_version(spark_cluster_version, client=client)
    _add_conf({
        "spark.yt.version": spyt_version,
        "spark.yt.jars": "yt:/{}".format(spyt_jar_path(spyt_version)),
        "spark.yt.pyFiles": "yt:/{}".format(spyt_python_path(spyt_version))
    }, spark_args)


def shell(discovery_path, spark_home, spark_args, spyt_version=None, client=None, spark_id=None):
    spark_shell_path = "{}/bin/spark-shell".format(spark_home)
    spark_base_args = [spark_shell_path]
    discovery = SparkDiscovery(discovery_path=discovery_path, spark_id=spark_id)
    _add_master(discovery, spark_base_args, rest=False, client=client)
    _add_base_spark_conf(client, discovery, spark_base_args)
    _add_conf({"spark.ui.showConsoleProgress": "true"}, spark_base_args)
    _add_spyt_deps(spyt_version, spark_base_args, discovery, client)
    spark_env = _create_spark_env(client, spark_home)

    os.execve(spark_shell_path, spark_base_args + spark_args, spark_env)


def get_spark_conf(config, enablers):
    dict_conf = config.get("spark_conf") or {}
    update_inplace(dict_conf, enablers.get_spark_conf())

    spark_conf = []
    for key, value in dict_conf.items():
        spark_conf.append("-D{}={}".format(key, value))

    return " ".join(spark_conf)


def build_spark_operation_spec(operation_alias, spark_discovery, config,
                               worker_cores, worker_memory, worker_num, worker_cores_overhead, worker_timeout,
                               tmpfs_limit, master_memory_limit, network_project, tvm_id, tvm_secret, history_server_memory_limit,
                               pool, enablers, client):
    def _launcher_command(component):
        unpack_tar = "tar --warning=no-unknown-keyword -xf spark.tgz -C ./tmpfs"
        move_java = "cp -r /opt/jdk11 ./tmpfs/jdk11"
        run_launcher = "./tmpfs/jdk11/bin/java -Xmx512m -cp spark-yt-launcher.jar"
        spark_conf = get_spark_conf(config=config, enablers=enablers)

        return "{0} && {1} && {2} {3} ru.yandex.spark.launcher.{4}Launcher ".format(unpack_tar, move_java, run_launcher,
                                                                                    spark_conf, component)

    master_command = _launcher_command("Master")
    worker_command = _launcher_command("Worker") + \
        "--cores {0} --memory {1} --wait-master-timeout {2}".format(worker_cores, worker_memory, worker_timeout)
    history_command = _launcher_command("HistoryServer") + "--log-path yt:/{}".format(spark_discovery.event_log())

    user = get_user_name(client=client)

    operation_spec = config["operation_spec"]
    operation_spec["stderr_table_path"] = str(spark_discovery.stderr())
    operation_spec["pool"] = pool
    if "title" not in operation_spec:
        operation_spec["title"] = operation_alias or "spark_{}".format(user)

    operation_spec["description"] = {
        "Spark over YT": {
            "discovery_path": spark_discovery.base_discovery_path,
            "version": config["cluster_version"],
            "enable_byop": enablers.enable_byop,
            "enable_arrow": enablers.enable_arrow,
            "enable_mtn": enablers.enable_mtn
        }
    }

    environment = config["environment"]
    environment["YT_PROXY"] = get_proxy_url(required=True, client=client)
    environment["YT_OPERATION_ALIAS"] = operation_spec["title"]
    environment["SPARK_DISCOVERY_PATH"] = str(spark_discovery.discovery())
    environment["JAVA_HOME"] = "$HOME/tmpfs/jdk11"
    environment["SPARK_HOME"] = "$HOME/tmpfs/spark"
    environment["SPARK_CLUSTER_VERSION"] = config["cluster_version"]
    environment["SPARK_YT_BYOP_PORT"] = "27002"

    ytserver_proxy_path = config.get("ytserver_proxy_path")
    ytserver_binary_name = ytserver_proxy_path.split("/")[-1] if ytserver_proxy_path else "ytserver-proxy"
    worker_environment = {
        "SPARK_YT_BYOP_ENABLED": str(enablers.enable_byop),
        "SPARK_YT_BYOP_BINARY_PATH": "$HOME/{}".format(ytserver_binary_name),
        "SPARK_YT_BYOP_CONFIG_PATH": "$HOME/ytserver-proxy.template.yson",
        "SPARK_YT_BYOP_HOST": "localhost",
        "SPARK_YT_BYOP_TVM_ENABLED": str(enablers.enable_mtn)
    }

    if enablers.enable_byop:
        worker_cores_overhead = worker_cores_overhead or SparkDefaultArguments.SPARK_WORKER_CORES_BYOP_OVERHEAD
    else:
        worker_cores_overhead = worker_cores_overhead or SparkDefaultArguments.SPARK_WORKER_CORES_OVERHEAD

    common_task_spec = {
        "restart_completed_jobs": True,
        "file_paths": config["file_paths"],
        "layer_paths": config["layer_paths"],
        "environment": environment,
        "memory_reserve_factor": 1.0,
        "tmpfs_path": "tmpfs"
    }

    worker_file_paths = copy.copy(common_task_spec["file_paths"])
    if ytserver_proxy_path and enablers.enable_byop:
        worker_file_paths.append(ytserver_proxy_path)
        operation_spec["description"]["BYOP"] = ytserver_proxy_attributes(ytserver_proxy_path, client=client)

    if enablers.enable_profiling:
        worker_file_paths.append("//home/sashbel/profiler.zip")

    secure_vault = {"YT_USER": user, "YT_TOKEN": get_token(client=client)}

    if enablers.enable_mtn:
        common_task_spec["network_project"] = network_project
        secure_vault["SPARK_TVM_ID"] = tvm_id
        secure_vault["SPARK_TVM_SECRET"] = tvm_secret

    return VanillaSpecBuilder() \
        .begin_task("master") \
            .job_count(1) \
            .command(master_command) \
            .memory_limit(_parse_memory(master_memory_limit)) \
            .cpu_limit(2) \
            .spec(common_task_spec) \
        .end_task() \
        .begin_task("history") \
            .job_count(1) \
            .command(history_command) \
            .memory_limit(_parse_memory(history_server_memory_limit)) \
            .cpu_limit(1) \
            .spec(common_task_spec) \
        .end_task() \
        .begin_task("workers") \
            .job_count(worker_num) \
            .command(worker_command) \
            .memory_limit(_parse_memory(worker_memory) + _parse_memory(tmpfs_limit)) \
            .cpu_limit(worker_cores + worker_cores_overhead) \
            .spec(common_task_spec) \
            .environment(update(environment, worker_environment)) \
            .file_paths(worker_file_paths) \
        .end_task() \
        .secure_vault(secure_vault) \
        .spec(operation_spec)


def start_spark_cluster(worker_cores, worker_memory, worker_num,
                        worker_cores_overhead=None,
                        worker_timeout=SparkDefaultArguments.SPARK_WORKER_TIMEOUT,
                        operation_alias=None, discovery_path=None, pool=None,
                        tmpfs_limit=SparkDefaultArguments.SPARK_WORKER_TMPFS_LIMIT,
                        master_memory_limit=SparkDefaultArguments.SPARK_MASTER_MEMORY_LIMIT,
                        history_server_memory_limit=SparkDefaultArguments.SPARK_HISTORY_SERVER_MEMORY_LIMIT,
                        params=None, spark_cluster_version=None, network_project=None, tvm_id=None, tvm_secret=None,
                        enablers=None,
                        client=None):
    """Start Spark cluster
    :param operation_alias: alias for the underlying YT operation
    :param pool: pool for the underlying YT operation
    :param discovery_path: Cypress path for discovery files and logs
    :param worker_cores: number of cores that will be available on worker
    :param worker_memory: amount of memory that will be available on worker
    :param worker_num: number of workers
    :param worker_cores_overhead: additional worker cores
    :param worker_timeout: timeout to fail master waiting
    :param tmpfs_limit: limit of tmpfs usage, default 150G
    :param master_memory_limit: memory limit for master, default 2G
    :param history_server_memory_limit: memory limit for history server, default 8G
    :param spark_cluster_version: Spark cluster version
    :param network_project: YT network project
    :param tvm_id: TVM id for network project
    :param tvm_secret: TVM secret for network project
    :param params: YT operation params: file_paths, layer_paths, operation_spec, environment, spark_conf
    :param enablers: ...
    :param client: YtClient
    :return:
    """
    spark_discovery = SparkDiscovery(discovery_path=discovery_path)

    ytserver_proxy_path = latest_ytserver_proxy_path(spark_cluster_version, client=client)
    global_conf = read_global_conf(client=client)
    spark_cluster_version = spark_cluster_version or latest_cluster_version(global_conf)

    validate_cluster_version(spark_cluster_version, client=client)
    validate_custom_params(params)
    validate_mtn_config(enablers, network_project, tvm_id, tvm_secret)

    dynamic_config = SparkDefaultArguments.get_params()
    update_config_inplace(dynamic_config, read_remote_conf(global_conf, spark_cluster_version, client=client))
    update_config_inplace(dynamic_config, params)
    if ytserver_proxy_path:
        dynamic_config["ytserver_proxy_path"] = ytserver_proxy_path

    enablers = enablers or SpytEnablers()
    enablers.apply_config(dynamic_config)

    spec_builder = build_spark_operation_spec(operation_alias=operation_alias,
                                              spark_discovery=spark_discovery,
                                              config=dynamic_config,
                                              worker_cores=worker_cores,
                                              worker_memory=worker_memory,
                                              worker_num=worker_num,
                                              worker_cores_overhead=worker_cores_overhead,
                                              worker_timeout=worker_timeout,
                                              tmpfs_limit=tmpfs_limit,
                                              master_memory_limit=master_memory_limit,
                                              network_project=network_project,
                                              tvm_id=tvm_id,
                                              tvm_secret=tvm_secret,
                                              history_server_memory_limit=history_server_memory_limit,
                                              pool=pool,
                                              enablers=enablers,
                                              client=client)

    spark_discovery.create(client)
    op = run_operation(spec_builder, sync=False, client=client)
    _wait_master_start(op, spark_discovery, client)
    master_address = SparkDiscovery.get(spark_discovery.master_webui(), client=client)
    logger.info("Spark Master's Web UI: http://{0}".format(master_address))

    return op


def find_spark_cluster(discovery_path=None, client=None):
    """Print Spark urls
    :param discovery_path: Cypress path for discovery files and logs
    :param client: YtClient
    :return: None
    """
    discovery = SparkDiscovery(discovery_path=discovery_path)
    return SparkCluster(
        master_endpoint=SparkDiscovery.get(discovery.master_spark(), client=client),
        master_web_ui_url=SparkDiscovery.get(discovery.master_webui(), client=client),
        master_rest_endpoint=SparkDiscovery.get(discovery.master_rest(), client=client),
        operation_id=SparkDiscovery.get(discovery.operation(), client=client),
        shs_url=SparkDiscovery.get(discovery.shs(), client=client),
        spark_cluster_version=SparkDiscovery.get(discovery.spark_cluster_version(), client=client)
    )
