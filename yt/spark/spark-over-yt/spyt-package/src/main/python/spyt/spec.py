import copy
import logging
import os
from typing import List, NamedTuple

from spyt.dependency_utils import require_yt_client

require_yt_client()

from yt.wrapper import YtClient  # noqa: E402
from yt.wrapper.common import update_inplace, update  # noqa: E402
from yt.wrapper.http_helpers import get_token, get_user_name  # noqa: E402
from yt.wrapper.spec_builders import VanillaSpecBuilder  # noqa: E402

from .conf import ytserver_proxy_attributes, get_spark_distributive  # noqa: E402
from .utils import SparkDiscovery, call_get_proxy_address_url, parse_memory  # noqa: E402
from .enabler import SpytEnablers  # noqa: E402
from .version import __version__  # noqa: E402

logger = logging.getLogger(__name__)


class SparkDefaultArguments(object):
    SPARK_WORKER_MEMORY_OVERHEAD = "2G"
    SPARK_WORKER_TMPFS_LIMIT = "8G"
    SPARK_WORKER_SSD_LIMIT = None
    SPARK_MASTER_PORT = 27001
    SPARK_WORKER_PORT = 27001
    SPARK_MASTER_MEMORY_LIMIT = "4G"
    SPARK_HISTORY_SERVER_MEMORY_LIMIT = "4G"
    SPARK_HISTORY_SERVER_MEMORY_OVERHEAD = "2G"
    SPARK_HISTORY_SERVER_CPU_LIMIT = 1
    SPARK_WORKER_TIMEOUT = "10m"
    SPARK_WORKER_LOG_UPDATE_INTERVAL = "10m"
    SPARK_WORKER_LOG_TABLE_TTL = "7d"
    SPARK_WORKER_CORES_OVERHEAD = 0
    SPARK_WORKER_CORES_BYOP_OVERHEAD = 0
    LIVY_DRIVER_CORES = 1
    LIVY_DRIVER_MEMORY = "1024m"
    LIVY_MAX_SESSIONS = 3

    @staticmethod
    def get_params():
        return {
            "operation_spec": {
                "annotations": {
                    "is_spark": True,
                    "solomon_resolver_tag": "spark",
                    "solomon_resolver_ports": [27100],
                },
                "max_failed_job_count": 10000,
                "max_stderr_count": 150,
                "job_cpu_monitor": {
                    "enable_cpu_reclaim": False
                }
            }
        }


class CommonComponentConfig(NamedTuple):
    operation_alias: str = None
    pool: str = None
    enable_tmpfs: bool = True
    network_project: str = None
    tvm_id: str = None
    tvm_secret: str = None
    enablers: SpytEnablers = None
    preemption_mode: str = "normal"
    cluster_log_level: str = "INFO"
    rpc_job_proxy: bool = False
    rpc_job_proxy_thread_pool_size: int = 4
    tcp_proxy_range_start: int = 30000
    tcp_proxy_range_size: int = 100
    enable_stderr_table: bool = False
    alias_prefix: str = ""


class MasterConfig(NamedTuple):
    master_memory_limit: str = SparkDefaultArguments.SPARK_MASTER_MEMORY_LIMIT
    master_port: int = SparkDefaultArguments.SPARK_MASTER_PORT


class WorkerResources(NamedTuple):
    cores: int
    memory: str
    num: int
    cores_overhead: int
    timeout: str
    memory_overhead: str


class WorkerConfig(NamedTuple):
    tmpfs_limit: str = SparkDefaultArguments.SPARK_WORKER_TMPFS_LIMIT
    res: WorkerResources = None
    worker_port: int = SparkDefaultArguments.SPARK_WORKER_PORT
    driver_op_discovery_script: str = None
    extra_metrics_enabled: bool = True
    autoscaler_enabled: bool = None
    worker_log_transfer: bool = False
    worker_log_json_mode: bool = False
    worker_log_update_interval: str = SparkDefaultArguments.SPARK_WORKER_LOG_UPDATE_INTERVAL
    worker_log_table_ttl: str = SparkDefaultArguments.SPARK_WORKER_LOG_TABLE_TTL
    worker_disk_name: str = "default"
    worker_disk_limit = None
    worker_disk_account = None


class HistoryServerConfig(NamedTuple):
    history_server_memory_limit: str = SparkDefaultArguments.SPARK_HISTORY_SERVER_MEMORY_LIMIT
    history_server_cpu_limit: int = SparkDefaultArguments.SPARK_HISTORY_SERVER_CPU_LIMIT
    history_server_memory_overhead: str = SparkDefaultArguments.SPARK_HISTORY_SERVER_MEMORY_OVERHEAD
    shs_location: str = None
    advanced_event_log: bool = False


class LivyConfig(NamedTuple):
    livy_driver_cores: int = SparkDefaultArguments.LIVY_DRIVER_CORES
    livy_driver_memory: str = SparkDefaultArguments.LIVY_DRIVER_MEMORY
    livy_max_sessions: int = SparkDefaultArguments.LIVY_MAX_SESSIONS
    spark_master_address: str = None


class CommonSpecParams(NamedTuple):
    spark_home: str
    spark_distributive_tgz: str
    java_home: str
    extra_java_opts: List[str]
    environment: dict
    spark_conf: dict
    task_spec: dict
    spark_discovery: SparkDiscovery
    config: CommonComponentConfig


def spark_conf_to_opts(config: dict):
    return ["-D{}={}".format(key, value) for key, value in config.items()]


def setup_spyt_env(spark_home: str, spark_distributive: str, additional_parameters: List[str]):
    cmd = ["./setup-spyt-env.sh", "--spark-home", spark_home, "--spark-distributive", spark_distributive]
    return " ".join(cmd + additional_parameters)


def _launcher_command(component: str, common_params: CommonSpecParams, additional_parameters: List[str] = None,
                      xmx: str = "512m", extra_java_opts: List[str] = None, launcher_opts: str = ""):
    additional_parameters = additional_parameters or []
    extra_java_opts = extra_java_opts or []
    setup_spyt_env_cmd = setup_spyt_env(common_params.spark_home,
                                        common_params.spark_distributive_tgz,
                                        additional_parameters)

    java_bin = os.path.join(common_params.java_home, 'bin', 'java')
    classpath = (f'{common_params.spark_home}/spyt-package/conf/:'
                 f'{common_params.spark_home}/spyt-package/jars/*:'
                 f'{common_params.spark_home}/spark/jars/*')
    extra_java_opts_str = " ".join(extra_java_opts)
    run_launcher = "{} -Xmx{} -cp {} {}".format(java_bin, xmx, classpath, extra_java_opts_str)

    commands = [
        setup_spyt_env_cmd,
        "{} tech.ytsaurus.spark.launcher.{}Launcher {}".format(run_launcher, component, launcher_opts)
    ]
    return " && ".join(commands)


def build_livy_spec(builder: VanillaSpecBuilder, common_params: CommonSpecParams, config: LivyConfig):
    livy_launcher_opts = [
        f"--driver-cores {config.livy_driver_cores}",
        f"--driver-memory {config.livy_driver_memory}",
        f"--max-sessions {config.livy_max_sessions}",
    ]
    if config.spark_master_address is not None:
        livy_launcher_opts.append(f"--master-address {config.spark_master_address}")
    extra_java_opts = common_params.extra_java_opts + spark_conf_to_opts(common_params.spark_conf)
    livy_command = _launcher_command("Livy", common_params, additional_parameters=["--enable-livy"],
                                     extra_java_opts=extra_java_opts, launcher_opts=" ".join(livy_launcher_opts))

    livy_environment = {
        "LIVY_HOME": "$HOME/{}/livy".format(common_params.spark_home),
    }
    livy_environment = update(common_params.environment, livy_environment)

    livy_file_paths = copy.copy(common_params.task_spec["file_paths"])
    livy_file_paths.append("//home/spark/livy/livy.tgz")

    livy_task_spec = copy.deepcopy(common_params.task_spec)
    livy_task_spec["environment"] = livy_environment

    builder.begin_task("livy") \
        .job_count(1) \
        .command(livy_command) \
        .memory_limit(parse_memory("1G") +
                      parse_memory(config.livy_driver_memory) * config.livy_max_sessions) \
        .cpu_limit(1 + config.livy_driver_cores * config.livy_max_sessions) \
        .spec(livy_task_spec) \
        .file_paths(livy_file_paths) \
        .end_task()


def build_hs_spec(builder: VanillaSpecBuilder, common_params: CommonSpecParams, config: HistoryServerConfig):
    spark_conf_hs = common_params.spark_conf.copy()
    if "spark.workerLog.tablePath" not in spark_conf_hs:
        worker_log_location = "yt:/{}".format(common_params.spark_discovery.worker_log())
        spark_conf_hs["spark.workerLog.tablePath"] = worker_log_location
    if config.advanced_event_log:
        event_log_path = "ytEventLog:/{}".format(config.shs_location or common_params.spark_discovery.event_log_table())
    else:
        event_log_path = "yt:/{}".format(config.shs_location or common_params.spark_discovery.event_log())
    if "spark.history.fs.numReplayThreads" not in spark_conf_hs:
        spark_conf_hs["spark.history.fs.numReplayThreads"] = config.history_server_cpu_limit

    history_launcher_opts = "--log-path {} --memory {}".format(event_log_path, config.history_server_memory_limit)
    extra_java_opts = common_params.extra_java_opts + spark_conf_to_opts(spark_conf_hs)
    history_command = _launcher_command("HistoryServer", common_params, extra_java_opts=extra_java_opts,
                                        launcher_opts=history_launcher_opts)

    shs_file_paths = copy.copy(common_params.task_spec["file_paths"])
    if common_params.config.enablers.enable_profiling:
        shs_file_paths.append("//home/sashbel/profiler.zip")
    builder.begin_task("history") \
        .job_count(1) \
        .command(history_command) \
        .memory_limit(parse_memory(config.history_server_memory_limit) +
                      parse_memory(config.history_server_memory_overhead)) \
        .cpu_limit(config.history_server_cpu_limit) \
        .spec(common_params.task_spec) \
        .file_paths(shs_file_paths) \
        .end_task()


def build_master_spec(builder: VanillaSpecBuilder, common_params: CommonSpecParams, config: MasterConfig):
    extra_java_opts = common_params.extra_java_opts + spark_conf_to_opts(common_params.spark_conf)
    master_command = _launcher_command("Master", common_params, extra_java_opts=extra_java_opts)
    master_environment = {
        "SPARK_MASTER_PORT": str(config.master_port),
    }
    master_environment = update(common_params.environment, master_environment)
    master_task_spec = copy.deepcopy(common_params.task_spec)
    master_task_spec["environment"] = master_environment
    builder.begin_task("master") \
        .job_count(1) \
        .command(master_command) \
        .memory_limit(parse_memory(config.master_memory_limit)) \
        .cpu_limit(2) \
        .spec(master_task_spec) \
        .end_task()


def build_worker_spec(builder: VanillaSpecBuilder, job_type: str, ytserver_proxy_path: str, tvm_enabled: bool,
                      common_params: CommonSpecParams, config: WorkerConfig):
    def _script_absolute_path(script):
        return "{}/{}".format(common_params.spark_home, script)

    spark_conf_worker = common_params.spark_conf.copy()

    spark_conf_worker['spark.shuffle.service.enabled'] = 'true'
    if "spark.workerLog.tablePath" not in common_params.spark_conf:
        worker_log_location = "yt:/{}".format(common_params.spark_discovery.worker_log())
        spark_conf_worker["spark.workerLog.tablePath"] = worker_log_location
    if config.driver_op_discovery_script:
        script_absolute_path = _script_absolute_path(config.driver_op_discovery_script)
        spark_conf_worker["spark.worker.resource.driverop.amount"] = str(config.res.cores)
        spark_conf_worker["spark.worker.resource.driverop.discoveryScript"] = script_absolute_path
        spark_conf_worker["spark.driver.resource.driverop.discoveryScript"] = script_absolute_path
    if config.extra_metrics_enabled:
        spark_conf_worker["spark.ui.prometheus.enabled"] = "true"

    if config.autoscaler_enabled:
        spark_conf_worker["spark.worker.resource.jobid.amount"] = "1"
        spark_conf_worker["spark.worker.resource.jobid.discoveryScript"] = _script_absolute_path(
            "spark/bin/job-id-discovery.sh")
        spark_conf_worker["spark.driver.resource.jobid.discoveryScript"] = _script_absolute_path(
            "spark/bin/job-id-discovery.sh")

    worker_launcher_opts = \
        "--cores {0} --memory {1} --wait-master-timeout {2} --wlog-service-enabled {3} --wlog-enable-json {4} " \
        "--wlog-update-interval {5} --wlog-table-ttl {6}".format(
            config.res.cores, config.res.memory,
            config.res.timeout, config.worker_log_transfer, config.worker_log_json_mode,
            config.worker_log_update_interval, config.worker_log_table_ttl)
    extra_java_opts = common_params.extra_java_opts + spark_conf_to_opts(spark_conf_worker)
    worker_command = _launcher_command("Worker", common_params, xmx="2g", extra_java_opts=extra_java_opts,
                                       launcher_opts=worker_launcher_opts)

    worker_environment = {
        "SPARK_YT_BYOP_ENABLED": str(common_params.config.enablers.enable_byop),
        "SPARK_WORKER_PORT": str(config.worker_port)
    }
    worker_environment = update(common_params.environment, worker_environment)
    if common_params.config.enablers.enable_byop:
        ytserver_binary_name = ytserver_proxy_path.split("/")[-1] if ytserver_proxy_path else "ytserver-proxy"
        byop_worker_environment = {
            "SPARK_YT_BYOP_BINARY_PATH": "$HOME/{}".format(ytserver_binary_name),
            "SPARK_YT_BYOP_CONFIG_PATH": "$HOME/ytserver-proxy.template.yson",
            "SPARK_YT_BYOP_HOST": "localhost",
            "SPARK_YT_BYOP_TVM_ENABLED": str(tvm_enabled)
        }
        worker_environment = update(worker_environment, byop_worker_environment)

    if common_params.config.enablers.enable_byop:
        worker_cores_overhead = config.res.cores_overhead or SparkDefaultArguments.SPARK_WORKER_CORES_BYOP_OVERHEAD
    else:
        worker_cores_overhead = config.res.cores_overhead or SparkDefaultArguments.SPARK_WORKER_CORES_OVERHEAD

    worker_file_paths = copy.copy(common_params.task_spec["file_paths"])
    if ytserver_proxy_path and common_params.config.enablers.enable_byop:
        worker_file_paths.append(ytserver_proxy_path)

    if common_params.config.enablers.enable_profiling:
        worker_file_paths.append("//home/sashbel/profiler.zip")

    worker_task_spec = copy.deepcopy(common_params.task_spec)
    worker_task_spec["environment"] = worker_environment
    worker_task_spec["rpc_proxy_worker_thread_pool_size"] = common_params.config.rpc_job_proxy_thread_pool_size
    worker_ram_memory = parse_memory(config.res.memory)
    worker_local_dirs = "."
    if config.worker_disk_limit:
        worker_task_spec["disk_request"] = {
            "disk_space": parse_memory(config.worker_disk_limit),
            "account": config.worker_disk_account,
            "medium_name": config.worker_disk_name
        }
    elif common_params.config.enable_tmpfs:
        worker_ram_memory += parse_memory(config.tmpfs_limit)
        worker_local_dirs = "./tmpfs"
    worker_environment["SPARK_LOCAL_DIRS"] = worker_local_dirs

    driver_task_spec = copy.deepcopy(worker_task_spec)
    driver_environment = driver_task_spec["environment"]
    if config.driver_op_discovery_script:
        driver_environment["SPARK_DRIVER_RESOURCE"] = str(config.res.cores)

    spec = driver_task_spec if "drivers" == job_type else worker_task_spec
    builder.begin_task(job_type) \
        .job_count(config.res.num) \
        .command(worker_command) \
        .memory_limit(worker_ram_memory + parse_memory(config.res.memory_overhead)) \
        .cpu_limit(config.res.cores + worker_cores_overhead) \
        .spec(spec) \
        .file_paths(worker_file_paths) \
        .end_task()


def build_spark_operation_spec(spark_discovery: SparkDiscovery, config: dict, client: YtClient,
                               job_types: List[str], common_config: CommonComponentConfig,
                               master_config: MasterConfig = None, worker_config: WorkerConfig = None,
                               hs_config: HistoryServerConfig = None, livy_config: LivyConfig = None):
    if job_types == [] or job_types is None:
        job_types = ['master', 'history', 'worker']

    spark_home = "./tmpfs" if common_config.enable_tmpfs else "."
    spark_distributive_tgz, spark_distributive_path = get_spark_distributive(client)

    extra_java_opts = ["-Dlog4j.loglevel={}".format(common_config.cluster_log_level)]
    if common_config.enablers.enable_preference_ipv6:
        extra_java_opts.append("-Djava.net.preferIPv6Addresses=true")

    spark_conf_common = config["spark_conf"].copy()
    update_inplace(spark_conf_common, common_config.enablers.get_conf())

    user = get_user_name(client=client)

    operation_spec = copy.copy(config["operation_spec"])
    if common_config.enable_stderr_table:
        if "master" in job_types:
            operation_spec["stderr_table_path"] = str(spark_discovery.stderr())
        elif "driver" in job_types:
            operation_spec["stderr_table_path"] = str(spark_discovery.stderr() + "_driver")
        else:
            operation_spec["stderr_table_path"] = str(spark_discovery.stderr()) + "_worker"
    if "title" not in operation_spec:
        operation_spec["title"] = common_config.operation_alias or (common_config.alias_prefix + "_" + str(user))
    operation_spec["pool"] = common_config.pool
    operation_spec['preemption_mode'] = common_config.preemption_mode
    operation_spec["description"] = {
        "Spark over YT": {
            "discovery_path": spark_discovery.base_discovery_path,
            "cluster_version": config["cluster_version"],
            "client_version": __version__,
            "enablers": str(common_config.enablers),
            "job_types": job_types
        }
    }
    ytserver_proxy_path = config.get("ytserver_proxy_path")
    if ytserver_proxy_path and common_config.enablers.enable_byop:
        operation_spec["description"]["BYOP"] = ytserver_proxy_attributes(ytserver_proxy_path, client=client)

    environment = copy.deepcopy(config["environment"])
    environment["YT_PROXY"] = call_get_proxy_address_url(required=True, client=client)
    environment["YT_OPERATION_ALIAS"] = operation_spec["title"]
    environment["SPARK_BASE_DISCOVERY_PATH"] = str(spark_discovery.base_discovery_path)
    environment["SPARK_DISCOVERY_PATH"] = str(spark_discovery.discovery())  # COMPAT(alex-shishkin)
    environment["SPARK_HOME"] = "$HOME/{}/spark".format(spark_home)
    environment["SPYT_HOME"] = "$HOME/{}/spyt-package".format(spark_home)
    environment["SPARK_CLUSTER_VERSION"] = config["cluster_version"]  # TODO Rename to SPYT_CLUSTER_VERSION
    environment["SPYT_CLUSTER_VERSION"] = config["cluster_version"]
    environment["SPARK_YT_CLUSTER_CONF_PATH"] = str(spark_discovery.conf())
    environment["SPARK_YT_SOLOMON_ENABLED"] = str(common_config.enablers.enable_solomon_agent)
    environment["SPARK_YT_IPV6_PREFERENCE_ENABLED"] = \
        str(common_config.enablers.enable_preference_ipv6)  # COMPAT(alex-shishkin)
    environment["SPARK_YT_TCP_PROXY_ENABLED"] = str(common_config.enablers.enable_tcp_proxy)
    environment["SPARK_YT_PROFILING_ENABLED"] = str(common_config.enablers.enable_profiling)
    environment["SPARK_YT_RPC_JOB_PROXY_ENABLED"] = str(common_config.rpc_job_proxy)
    if common_config.enablers.enable_byop:
        environment["SPARK_YT_BYOP_PORT"] = "27002"
    if common_config.enablers.enable_solomon_agent:
        environment["SOLOMON_PUSH_PORT"] = "27099"
    if common_config.enablers.enable_tcp_proxy:
        environment["SPARK_YT_TCP_PROXY_RANGE_START"] = str(common_config.tcp_proxy_range_start)
        environment["SPARK_YT_TCP_PROXY_RANGE_SIZE"] = str(common_config.tcp_proxy_range_size)

    file_paths = copy.copy(config["file_paths"])
    file_paths.append(spark_distributive_path)

    common_task_spec = {
        "restart_completed_jobs": True,
        "file_paths": file_paths,
        "layer_paths": config["layer_paths"],
        "environment": environment,
        "memory_reserve_factor": 1.0,
        "enable_rpc_proxy_in_job_proxy": common_config.rpc_job_proxy,
    }
    if common_config.enable_tmpfs:
        common_task_spec["tmpfs_path"] = "tmpfs"
    if common_config.enablers.enable_mtn:
        common_task_spec["network_project"] = common_config.network_project

    tvm_enabled = common_config.enablers.enable_mtn and bool(common_config.tvm_id) and bool(common_config.tvm_secret)
    secure_vault = {"YT_USER": user, "YT_TOKEN": get_token(client=client)}
    if tvm_enabled:
        secure_vault["SPARK_TVM_ID"] = common_config.tvm_id
        secure_vault["SPARK_TVM_SECRET"] = common_config.tvm_secret

    common_params = CommonSpecParams(
        spark_home, spark_distributive_tgz, environment["JAVA_HOME"], extra_java_opts,
        environment, spark_conf_common, common_task_spec, spark_discovery, common_config
    )
    builder = VanillaSpecBuilder()
    if "master" in job_types:
        build_master_spec(builder, common_params, master_config)
    if "history" in job_types:
        build_hs_spec(builder, common_params, hs_config)
    if "worker" in job_types:
        build_worker_spec(builder, "workers", ytserver_proxy_path, tvm_enabled, common_params, worker_config)
    if "driver" in job_types:
        build_worker_spec(builder, "drivers", ytserver_proxy_path, tvm_enabled, common_params, worker_config)
    if "livy" in job_types:
        build_livy_spec(builder, common_params, livy_config)

    return builder \
        .secure_vault(secure_vault) \
        .spec(operation_spec)
