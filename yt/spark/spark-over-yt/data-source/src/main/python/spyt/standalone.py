from collections import namedtuple
import copy
import logging
import os
import re
import subprocess
import uuid

from yt.wrapper.common import update_inplace, update
from yt.wrapper.cypress_commands import exists, move
from yt.wrapper.acl_commands import check_permission
from yt.wrapper.file_commands import upload_file_to_cache
from yt.wrapper.http_helpers import get_token, get_user_name, get_proxy_url
from yt.wrapper.operation_commands import TimeWatcher, process_operation_unsuccesful_finish_state, \
    abort_operation, get_operation_state
from yt.wrapper.run_operation_commands import run_operation
from yt.wrapper.spec_builders import VanillaSpecBuilder

from .conf import read_remote_conf, validate_cluster_version, spyt_jar_path, spyt_python_path, \
    validate_spyt_version, validate_versions_compatibility, latest_compatible_spyt_version, \
    latest_cluster_version, update_config_inplace, validate_custom_params, validate_mtn_config, \
    latest_ytserver_proxy_path, ytserver_proxy_attributes, read_global_conf, python_bin_path, \
    worker_num_limit, validate_worker_num, read_cluster_conf, validate_ssd_config
from .utils import get_spark_master, base_spark_conf, SparkDiscovery, SparkCluster
from .enabler import SpytEnablers
from .version import __version__

logger = logging.getLogger(__name__)


class SparkDefaultArguments(object):
    SPARK_WORKER_TMPFS_LIMIT = "150G"
    SPARK_WORKER_SSD_LIMIT = None
    SPARK_MASTER_MEMORY_LIMIT = "6G"
    SPARK_HISTORY_SERVER_MEMORY_LIMIT = "16G"
    SPARK_HISTORY_SERVER_MEMORY_OVERHEAD = "4G"
    SPARK_HISTORY_SERVER_CPU_LIMIT = 8
    SPARK_WORKER_TIMEOUT = "10m"
    SPARK_WORKER_LOG_UPDATE_INTERVAL = "10m"
    SPARK_WORKER_LOG_TABLE_TTL = "7d"
    SPARK_WORKER_CORES_OVERHEAD = 0
    SPARK_WORKER_CORES_BYOP_OVERHEAD = 0

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


Worker = namedtuple(
    'Worker', ['cores', 'memory', 'num', 'cores_overhead', 'timeout'])


def _add_conf(spark_conf, spark_args):
    if spark_conf:
        for k, v in spark_conf.items():
            spark_args.append("--conf")
            spark_args.append("{}={}".format(k, v))


def _add_master(discovery, spark_args, rest, client=None):
    spark_args.append("--master")
    spark_args.append(get_spark_master(
        discovery, rest=False, yt_client=client))
    _add_conf({
        "spark.rest.master": get_spark_master(discovery, rest=True, yt_client=client)
    }, spark_args)


def _add_shs_option(discovery, spark_args, client=None):
    shs = SparkDiscovery.getOption(discovery.shs(), client=client)
    if shs is not None:
        _add_conf({
            "spark.rest.shs": shs
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
    units = {"gb": 1024 * 1024 * 1024, "mb": 1024 *
             1024, "kb": 1024, "bb": 1, "b": 1}
    if memory_str is None:
        return None
    m = re.match(r"(\d+)(.*)", memory_str)
    value = int(m.group(1))
    unit = m.group(2).lower().strip()
    if len(unit) <= 1:
        unit = unit + "b"
    return value * units[unit]


def _parse_bool(flag):
    return flag is not None and flag.lower() == 'true'


def _wait_op_start(op, operation_path, client):
    for state in op.get_state_monitor(TimeWatcher(1.0, 1.0, 0.0)):
        if state.is_running() and exists(operation_path, client=client):
            return op
        elif state.is_unsuccessfully_finished():
            process_operation_unsuccesful_finish_state(op, op.get_error(state))
        else:
            op.printer(state)


def _wait_child_start(op, spark_discovery, client):
    _wait_op_start(
        op, spark_discovery.children_operations().join(op.id), client)


def _wait_master_start(op, spark_discovery, client):
    _wait_op_start(op, spark_discovery.operation().join(op.id), client)


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


def raw_submit(discovery_path, spark_home, spark_args, spyt_version=None,
               python_version=None, local_files=True, client=None):
    spark_submit_path = "{}/bin/spark-submit".format(spark_home)
    spark_base_args = [spark_submit_path]
    permission_status = check_permission(user=client.get_user_name(),
                                         permission='read', path=discovery_path, client=client)
    if permission_status.get('action', 'deny') != 'allow':
        raise RuntimeError(
            'No permission for reading cluster, actual permission status is ' + str(permission_status))
    discovery = SparkDiscovery(discovery_path=discovery_path)

    spark_conf = read_cluster_conf(str(discovery.conf()), client)['spark_conf']
    dedicated_driver_op = _parse_bool(spark_conf.get('spark.dedicated_operation_mode'))
    jar_caching_enabled = _parse_bool(spark_conf.get('spark.yt.jarCaching'))

    _add_master(discovery, spark_base_args, rest=True, client=client)
    _add_shs_option(discovery, spark_base_args, client=client)
    _add_base_spark_conf(client, discovery, spark_base_args)
    _add_spyt_deps(spyt_version, spark_base_args, discovery, client, jar_caching_enabled)
    _add_python_version(python_version, spark_base_args, client)
    _add_dedicated_driver_op_conf(spark_base_args, dedicated_driver_op)
    spark_env = _create_spark_env(client, spark_home)

    if local_files:
        remote_paths = {}
        new_spark_args = []
        for spark_arg in spark_args:
            if spark_arg.startswith('local:/'):
                if spark_arg not in remote_paths:
                    file_path = spark_arg[7:] # Drops prefix
                    _, file_extension = os.path.splitext(file_path)
                    destination = upload_file_to_cache(file_path, client=client)
                    destination_ext = "{}{}".format(destination, file_extension)
                    move(destination, destination_ext, client=client) # Extension is necessary
                    logger.info("%s has been uploaded to YT as %s", file_path, destination_ext)
                    remote_paths[spark_arg] = "yt:/{}".format(destination_ext)
                new_spark_args.append(remote_paths[spark_arg])
            else:
                new_spark_args.append(spark_arg)
        spark_args = new_spark_args

    # replace stdin to avoid https://bugs.openjdk.java.net/browse/JDK-8211842
    return subprocess.call(spark_base_args + spark_args, env=spark_env, stdin=subprocess.PIPE)


def _add_dedicated_driver_op_conf(spark_args, dedicated_driver_op):
    if dedicated_driver_op:
        _add_conf({
            "spark.driver.resource.driverop.amount": 1
        }, spark_args)


def _add_python_version(python_version, spark_args, client):
    if python_version is not None:
        global_conf = read_global_conf(client=client)
        python_path = python_bin_path(global_conf, python_version)
        if python_path:
            _add_conf({
                "spark.pyspark.python": python_path
            }, spark_args)
        else:
            raise RuntimeError(
                "Interpreter for python version `{}` is not found".format(python_version))


def wrap_cached_jar(path, jar_caching_enabled):
    if jar_caching_enabled and path.startswith("yt:/"):
        return "ytCached:/" + path[4:]
    else:
        return path


def _add_spyt_deps(spyt_version, spark_args, discovery, client, jar_caching_enabled):
    spark_cluster_version = SparkDiscovery.get(
        discovery.spark_cluster_version(), client=client)
    if spyt_version is not None:
        validate_spyt_version(spyt_version, client=client)
        validate_versions_compatibility(spyt_version, spark_cluster_version)
    else:
        spyt_version = latest_compatible_spyt_version(
            spark_cluster_version, client=client)
    _add_conf({
        "spark.yt.version": spyt_version,
        "spark.yt.jars": wrap_cached_jar("yt:/{}".format(spyt_jar_path(spyt_version)), jar_caching_enabled),
        "spark.yt.pyFiles": wrap_cached_jar("yt:/{}".format(spyt_python_path(spyt_version)), jar_caching_enabled)
    }, spark_args)


def shell(discovery_path, spark_home, spark_args, spyt_version=None, client=None):
    spark_shell_path = "{}/bin/spark-shell".format(spark_home)
    spark_base_args = [spark_shell_path]
    discovery = SparkDiscovery(discovery_path=discovery_path)

    spark_conf = read_cluster_conf(str(discovery.conf()), client)['spark_conf']
    jar_caching_enabled = _parse_bool(spark_conf.get('spark.yt.jarCaching'))

    _add_master(discovery, spark_base_args, rest=False, client=client)
    _add_shs_option(discovery, spark_base_args, client=client)
    _add_base_spark_conf(client, discovery, spark_base_args)
    _add_conf({"spark.ui.showConsoleProgress": "true"}, spark_base_args)
    _add_spyt_deps(spyt_version, spark_base_args, discovery, client, jar_caching_enabled)
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
                               worker,
                               tmpfs_limit, ssd_limit, ssd_account,
                               master_memory_limit, shs_location,
                               history_server_memory_limit, history_server_memory_overhead, history_server_cpu_limit,
                               network_project, tvm_id, tvm_secret,
                               advanced_event_log, worker_log_transfer, worker_log_json_mode,
                               worker_log_update_interval, worker_log_table_ttl,
                               pool, enablers, client,
                               preemption_mode, job_types, driver_op_resources=None, driver_op_discovery_script=None,
                               extra_metrics_enabled=True, autoscaler_enabled=False):
    if job_types == [] or job_types == None:
        job_types = ['master', 'history', 'worker']

    if ssd_limit:
        spark_home = "."
    else:
        spark_home = "./tmpfs"

    def script_path(script):
        return "{}/{}".format(spark_home, driver_op_discovery_script)

    def _launcher_command(component, xmx="512m"):
        create_dir = "mkdir -p {}".format(spark_home)
        unpack_tar = "tar --warning=no-unknown-keyword -xf spark.tgz -C {}".format(
            spark_home)
        move_java = "cp -r /opt/jdk11 ./tmpfs/jdk11"
        run_launcher = "./tmpfs/jdk11/bin/java -Xmx{0} -cp spark-yt-launcher.jar -Djava.net.preferIPv6Addresses=true".format(
            xmx)
        spark_conf = get_spark_conf(config=config, enablers=enablers)

        return "{5} && {0} && {1} && {2} {3} tech.ytsaurus.spark.launcher.{4}Launcher ".format(
            unpack_tar, move_java, run_launcher, spark_conf, component, create_dir)

    master_command = _launcher_command("Master")

    def _script_absolute_path(script):
        return "{}/{}".format(spark_home, script)

    worker_log_location = "yt:/{}".format(spark_discovery.worker_log())
    if "spark.workerLog.tablePath" not in config["spark_conf"]:
        config["spark_conf"]["spark.workerLog.tablePath"] = worker_log_location
    if driver_op_discovery_script:
        script_absolute_path = _script_absolute_path(driver_op_discovery_script)
        config["spark_conf"]["spark.worker.resource.driverop.amount"] = str(
            driver_op_resources)
        config["spark_conf"]["spark.worker.resource.driverop.discoveryScript"] = script_absolute_path
        config["spark_conf"]["spark.driver.resource.driverop.discoveryScript"] = script_absolute_path
    if extra_metrics_enabled:
        config["spark_conf"]["spark.ui.prometheus.enabled"] = "true"

    if autoscaler_enabled:
        config["spark_conf"]["spark.worker.resource.jobid.amount"] = "1"
        config["spark_conf"]["spark.worker.resource.jobid.discoveryScript"] = _script_absolute_path(
            "spark/bin/job-id-discovery.sh")
        config["spark_conf"]["spark.driver.resource.jobid.discoveryScript"] = _script_absolute_path(
            "spark/bin/job-id-discovery.sh")

    worker_command = _launcher_command("Worker", "2g") + \
        "--cores {0} --memory {1} --wait-master-timeout {2} --wlog-service-enabled {3} " \
        "--wlog-enable-json {4} --wlog-update-interval {5} --wlog-table-path {6} " \
        "--wlog-table-ttl {7}".format(
            worker.cores, worker.memory, worker.timeout, worker_log_transfer, worker_log_json_mode,
            worker_log_update_interval, worker_log_location, worker_log_table_ttl)

    if advanced_event_log:
        event_log_path = "ytEventLog:/{}".format(
            shs_location or spark_discovery.event_log_table())
    else:
        event_log_path = "yt:/{}".format(
            shs_location or spark_discovery.event_log())
    if "spark.history.fs.numReplayThreads" not in config["spark_conf"]:
        config["spark_conf"]["spark.history.fs.numReplayThreads"] = history_server_cpu_limit
    history_command = _launcher_command("HistoryServer") + \
        "--log-path {} --memory {}".format(event_log_path,
                                           history_server_memory_limit)

    user = get_user_name(client=client)

    operation_spec = config["operation_spec"]

    if "master" in job_types:
        operation_spec["stderr_table_path"] = str(spark_discovery.stderr())
    elif "driver" in job_types:
        operation_spec["stderr_table_path"] = str(
            spark_discovery.stderr() + "_driver")
    else:
        operation_spec["stderr_table_path"] = str(
            spark_discovery.stderr()) + "_worker"

    operation_spec["pool"] = pool
    if "title" not in operation_spec:
        operation_spec["title"] = operation_alias or "spark_{}".format(user)

    operation_spec["description"] = {
        "Spark over YT": {
            "discovery_path": spark_discovery.base_discovery_path,
            "cluster_version": config["cluster_version"],
            "client_version": __version__,
            "enable_byop": enablers.enable_byop,
            "enable_arrow": enablers.enable_arrow,
            "enable_mtn": enablers.enable_mtn,
            "job_types": job_types
        }
    }

    operation_spec['preemption_mode'] = preemption_mode

    environment = config["environment"]
    environment["YT_PROXY"] = get_proxy_url(required=True, client=client)
    environment["YT_OPERATION_ALIAS"] = operation_spec["title"]
    environment["SPARK_BASE_DISCOVERY_PATH"] = str(
        spark_discovery.base_discovery_path)
    environment["SPARK_DISCOVERY_PATH"] = str(spark_discovery.discovery())
    environment["JAVA_HOME"] = "$HOME/tmpfs/jdk11"
    environment["SPARK_HOME"] = "$HOME/{}/spark".format(spark_home)
    environment["SPARK_CLUSTER_VERSION"] = config["cluster_version"]
    environment["SPARK_YT_CLUSTER_CONF_PATH"] = str(spark_discovery.conf())
    environment["SPARK_YT_BYOP_PORT"] = "27002"
    environment["SPARK_LOCAL_DIRS"] = "./tmpfs"
    environment["SPARK_YT_SOLOMON_ENABLED"] = str(enablers.enable_solomon_agent)
    environment["SOLOMON_PUSH_PORT"] = "27099"

    ytserver_proxy_path = config.get("ytserver_proxy_path")

    worker_environment = {
        "SPARK_YT_BYOP_ENABLED": str(enablers.enable_byop)
    }
    worker_environment = update(environment, worker_environment)
    if enablers.enable_byop:
        ytserver_binary_name = ytserver_proxy_path.split(
            "/")[-1] if ytserver_proxy_path else "ytserver-proxy"
        byop_worker_environment = {
            "SPARK_YT_BYOP_BINARY_PATH": "$HOME/{}".format(ytserver_binary_name),
            "SPARK_YT_BYOP_CONFIG_PATH": "$HOME/ytserver-proxy.template.yson",
            "SPARK_YT_BYOP_HOST": "localhost",
            "SPARK_YT_BYOP_TVM_ENABLED": str(enablers.enable_mtn)
        }
        worker_environment = update(worker_environment, byop_worker_environment)

    if enablers.enable_byop:
        worker_cores_overhead = worker.cores_overhead or SparkDefaultArguments.SPARK_WORKER_CORES_BYOP_OVERHEAD
    else:
        worker_cores_overhead = worker.cores_overhead or SparkDefaultArguments.SPARK_WORKER_CORES_OVERHEAD

    common_task_spec = {
        "restart_completed_jobs": True,
        "file_paths": config["file_paths"],
        "layer_paths": config["layer_paths"],
        "environment": environment,
        "memory_reserve_factor": 1.0,
        "tmpfs_path": "tmpfs"
    }

    worker_file_paths = copy.copy(common_task_spec["file_paths"])
    shs_file_paths = copy.copy(common_task_spec["file_paths"])
    if ytserver_proxy_path and enablers.enable_byop:
        worker_file_paths.append(ytserver_proxy_path)
        operation_spec["description"]["BYOP"] = ytserver_proxy_attributes(
            ytserver_proxy_path, client=client)

    if enablers.enable_profiling:
        worker_file_paths.append("//home/sashbel/profiler.zip")
        shs_file_paths.append("//home/sashbel/profiler.zip")

    secure_vault = {"YT_USER": user, "YT_TOKEN": get_token(client=client)}

    if enablers.enable_mtn:
        common_task_spec["network_project"] = network_project
        secure_vault["SPARK_TVM_ID"] = tvm_id
        secure_vault["SPARK_TVM_SECRET"] = tvm_secret

    worker_task_spec = copy.deepcopy(common_task_spec)
    worker_task_spec["environment"] = worker_environment
    if ssd_limit:
        worker_task_spec["disk_request"] = {
            "disk_space": _parse_memory(ssd_limit),
            "account": ssd_account,
            "medium_name": "ssd_slots_physical"
        }
        worker_environment["SPARK_LOCAL_DIRS"] = "."

    driver_task_spec = copy.deepcopy(worker_task_spec)
    driver_environment = driver_task_spec["environment"]
    if driver_op_resources:
        driver_environment["SPARK_DRIVER_RESOURCE"] = str(driver_op_resources)

    builder = VanillaSpecBuilder()
    if "master" in job_types:
        builder.begin_task("master") \
            .job_count(1) \
            .command(master_command) \
            .memory_limit(_parse_memory(master_memory_limit)) \
            .cpu_limit(2) \
            .spec(common_task_spec) \
            .end_task()
    if "history" in job_types:
        builder.begin_task("history") \
            .job_count(1) \
            .command(history_command) \
            .memory_limit(_parse_memory(history_server_memory_limit) + _parse_memory(history_server_memory_overhead)) \
            .cpu_limit(history_server_cpu_limit) \
            .spec(common_task_spec) \
            .file_paths(shs_file_paths) \
            .end_task()
    if "worker" in job_types:
        builder.begin_task("workers") \
            .job_count(worker.num) \
            .command(worker_command) \
            .memory_limit(_parse_memory(worker.memory) + _parse_memory(tmpfs_limit)) \
            .cpu_limit(worker.cores + worker_cores_overhead) \
            .spec(worker_task_spec) \
            .file_paths(worker_file_paths) \
            .end_task()
    if "driver" in job_types:
        builder.begin_task("drivers") \
            .job_count(worker.num) \
            .command(worker_command) \
            .memory_limit(_parse_memory(worker.memory) + _parse_memory(tmpfs_limit)) \
            .cpu_limit(worker.cores + worker_cores_overhead) \
            .spec(driver_task_spec) \
            .file_paths(worker_file_paths) \
            .end_task()

    return builder \
        .secure_vault(secure_vault) \
        .spec(operation_spec)


def stop_spark_cluster(discovery_path, client):
    """Stop Spark cluster
    :param discovery_path: Cypress path for discovery files and logs
    :param client: YtClient
    """
    spark_discovery = SparkDiscovery(discovery_path=discovery_path)
    abort_spark_operations(spark_discovery, client)


def abort_spark_operations(spark_discovery, client):
    current_operation_id = SparkDiscovery.getOption(
        spark_discovery.operation(), client=client)
    error = None
    if current_operation_id is not None and get_operation_state(
            current_operation_id, client=client).is_running():
        error = abort_operation_silently(current_operation_id, client=client)
        for child_id in SparkDiscovery.getOptions(spark_discovery.children_operations(), client):
            if get_operation_state(child_id, client=client).is_running():
                abort_operation_silently(child_id, client=client)
    if error:
        raise error


def start_spark_cluster(worker_cores, worker_memory, worker_num,
                        worker_cores_overhead=None,
                        worker_timeout=SparkDefaultArguments.SPARK_WORKER_TIMEOUT,
                        operation_alias=None, discovery_path=None, pool=None,
                        tmpfs_limit=SparkDefaultArguments.SPARK_WORKER_TMPFS_LIMIT,
                        ssd_limit=SparkDefaultArguments.SPARK_WORKER_SSD_LIMIT,
                        ssd_account=None,
                        master_memory_limit=SparkDefaultArguments.SPARK_MASTER_MEMORY_LIMIT,
                        history_server_memory_limit=SparkDefaultArguments.SPARK_HISTORY_SERVER_MEMORY_LIMIT,
                        history_server_cpu_limit=SparkDefaultArguments.SPARK_HISTORY_SERVER_CPU_LIMIT,
                        history_server_memory_overhead=SparkDefaultArguments.SPARK_HISTORY_SERVER_MEMORY_OVERHEAD,
                        network_project=None, abort_existing=False, tvm_id=None, tvm_secret=None,
                        advanced_event_log=False, worker_log_transfer=False, worker_log_json_mode=False,
                        worker_log_update_interval=SparkDefaultArguments.SPARK_WORKER_LOG_UPDATE_INTERVAL,
                        worker_log_table_ttl=SparkDefaultArguments.SPARK_WORKER_LOG_TABLE_TTL,
                        params=None, shs_location=None, spark_cluster_version=None, enablers=None, client=None,
                        preemption_mode="normal", enable_multi_operation_mode=False,
                        dedicated_operation_mode=False,
                        driver_cores=None, driver_memory=None, driver_num=None,
                        driver_cores_overhead=None, driver_timeout=None,
                        autoscaler_period=None, autoscaler_metrics_port=None,
                        autoscaler_sliding_window=None, autoscaler_max_free_workers=None,
                        autoscaler_slot_increment_step=None):
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
    :param ssd_limit: limit of ssd usage, default None, ssd disabled
    :param ssd_account: account for ssd quota
    :param master_memory_limit: memory limit for master, default 2G
    :param history_server_memory_limit: memory limit for history server, default 16G,
    total memory for SHS job is history_server_memory_limit + history_server_memory_overhead
    :param history_server_cpu_limit: cpu limit for history server, default 20
    :param history_server_memory_overhead: memory overhead for history server, default 2G,
    total memory for SHS job is history_server_memory_limit + history_server_memory_overhead
    :param spark_cluster_version: Spark cluster version
    :param network_project: YT network project
    :param abort_existing: abort existing running operation
    :param advanced_event_log: advanced log format for history server (requires dynamic tables write permission)
    :param worker_log_transfer: sending logs from workers to yt
    :param worker_log_json_mode: using json for worker logs
    :param worker_log_update_interval: intervals between log updates
    :param worker_log_table_ttl: TTL of yt table with worker logs
    :param tvm_id: TVM id for network project
    :param tvm_secret: TVM secret for network project
    :param params: YT operation params: file_paths, layer_paths, operation_spec, environment, spark_conf
    :param shs_location: hard set path to log directory
    :param enablers: ...
    :param client: YtClient
    :param preemption_mode: 'normal' or 'graceful' for graceful preemption
    :param enable_multi_operation_mode: use several vanilla operations for one cluster
    :param dedicated_operation_mode: use dedicated operation for drivers
    :param driver_cores: number of cores that will be available on driver worker
    :param driver_memory: amount of memory that will be available on driver worker
    :param driver_num: number of driver workers
    :param driver_cores_overhead: additional driver worker cores
    :param driver_timeout: timeout to fail master waiting
    :param autoscaler_period: time between autoscaler calls in scala duration string format
    :param autoscaler_metrics_port: port for exposing of autoscaler metrics (deprecated, not used)
    :param autoscaler_sliding_window: size of autoscaler actions sliding window (in number of action) to downscale
    :param autoscaler_max_free_workers: maximum number of free workers
    :param autoscaler_slot_increment_step: autoscaler workers increment step
    :return:
    """
    worker = Worker(worker_cores, worker_memory, worker_num,
                    worker_cores_overhead, worker_timeout)
    driver = Worker(driver_cores or worker_cores, driver_memory or worker_memory,
                    driver_num or worker_num, driver_cores_overhead or worker_cores_overhead,
                    driver_timeout or worker_timeout)
    dedicated_operation_mode = dedicated_operation_mode and driver_num > 0

    spark_discovery = SparkDiscovery(discovery_path=discovery_path)

    current_operation_id = SparkDiscovery.getOption(
        spark_discovery.operation(), client=client)

    if current_operation_id is not None and get_operation_state(
        current_operation_id, client=client).is_running():
        if abort_existing:
            abort_spark_operations(spark_discovery, client)
        else:
            raise RuntimeError(
                "This spark cluster is started already, use --abort-existing for auto restarting")

    ytserver_proxy_path = latest_ytserver_proxy_path(
        spark_cluster_version, client=client)
    global_conf = read_global_conf(client=client)
    spark_cluster_version = spark_cluster_version or latest_cluster_version(
        global_conf)

    validate_cluster_version(spark_cluster_version, client=client)
    validate_custom_params(params)
    validate_mtn_config(enablers, network_project, tvm_id, tvm_secret)
    validate_worker_num(worker.num, worker_num_limit(global_conf))
    validate_ssd_config(ssd_limit, ssd_account)

    dynamic_config = SparkDefaultArguments.get_params()
    update_config_inplace(dynamic_config, read_remote_conf(
        global_conf, spark_cluster_version, client=client))
    update_config_inplace(dynamic_config, params)
    dynamic_config['spark_conf']['spark.base.discovery.path'] = spark_discovery.base_discovery_path
    if ytserver_proxy_path:
        dynamic_config["ytserver_proxy_path"] = ytserver_proxy_path
    dynamic_config['spark_conf']['spark.dedicated_operation_mode'] = dedicated_operation_mode

    if autoscaler_period:
        dynamic_config['spark_conf']['spark.autoscaler.enabled'] = True
        dynamic_config['spark_conf']['spark.autoscaler.period'] = autoscaler_period
        if autoscaler_sliding_window:
            dynamic_config['spark_conf']['spark.autoscaler.sliding_window_size'] = autoscaler_sliding_window
        if autoscaler_max_free_workers:
            dynamic_config['spark_conf']['spark.autoscaler.max_free_workers'] = autoscaler_max_free_workers
        if autoscaler_slot_increment_step:
            dynamic_config['spark_conf']['spark.autoscaler.slots_increment_step'] = autoscaler_slot_increment_step

    enablers = enablers or SpytEnablers()
    enablers.apply_config(dynamic_config)

    spark_discovery.create(client)

    args = {
        'operation_alias': operation_alias,
        'spark_discovery': spark_discovery,
        'config': dynamic_config,
        'worker': worker,
        'tmpfs_limit': tmpfs_limit,
        'ssd_limit': ssd_limit,
        'ssd_account': ssd_account,
        'master_memory_limit': master_memory_limit,
        'shs_location': shs_location,
        'history_server_memory_limit': history_server_memory_limit,
        'history_server_memory_overhead': history_server_memory_overhead,
        'history_server_cpu_limit': history_server_cpu_limit,
        'network_project': network_project,
        'tvm_id': tvm_id,
        'tvm_secret': tvm_secret,
        'advanced_event_log': advanced_event_log,
        'worker_log_transfer': worker_log_transfer,
        'worker_log_json_mode': worker_log_json_mode,
        'worker_log_update_interval': worker_log_update_interval,
        'worker_log_table_ttl': worker_log_table_ttl,
        'pool': pool,
        'enablers': enablers,
        'client': client,
        'preemption_mode': preemption_mode,
        'autoscaler_enabled': autoscaler_period is not None,
        'job_types': ['master', 'history', 'worker']
    }

    master_args = args.copy()
    if enable_multi_operation_mode:
        master_args['job_types'] = ['master', 'history']
    master_builder = build_spark_operation_spec(**master_args)

    op_child = None
    op_driver = None
    op = None

    try:
        op = run_operation(master_builder, sync=False, client=client)
        _wait_master_start(op, spark_discovery, client)
        logger.info("Master operation %s", op.id)

        if enable_multi_operation_mode:
            child_args = args.copy()
            child_args['job_types'] = ['worker']
            child_builder = build_spark_operation_spec(**child_args)
            op_child = run_operation(child_builder, sync=False, client=client)
            _wait_child_start(op_child, spark_discovery, client)
            logger.info("Child operation %s", op_child.id)

        if dedicated_operation_mode:
            driver_args = args.copy()
            driver_args['job_types'] = ['driver']
            driver_args['worker'] = driver
            driver_args['driver_op_resources'] = driver.cores
            driver_args['driver_op_discovery_script'] = 'spark/bin/driver-op-discovery.sh'
            driver_builder = build_spark_operation_spec(**driver_args)
            op_driver = run_operation(driver_builder, sync=False, client=client)
            _wait_child_start(op_driver, spark_discovery, client)
            logger.info("Driver operation %s", op_driver.id)

        master_address = SparkDiscovery.get(
            spark_discovery.master_webui(), client=client)
        logger.info("Spark Master's Web UI: http://{0}".format(master_address))
        return op
    except Exception as err:
        logging.error(err, exc_info=True)
        abort_operation_silently(op_driver, client)
        abort_operation_silently(op_child, client)
        abort_operation_silently(op, client)


def abort_operation_silently(op_id, client):
    try:
        if op_id:
            abort_operation(op_id, client=client)
    except Exception as err:
        logging.error("Failed to abort operation {0}".format(op_id), exc_info=True)
        return err


def find_spark_cluster(discovery_path=None, client=None):
    """Print Spark urls
    :param discovery_path: Cypress path for discovery files and logs
    :param client: YtClient
    :return: None
    """
    discovery = SparkDiscovery(discovery_path=discovery_path)
    return SparkCluster(
        master_endpoint=SparkDiscovery.getOption(
            discovery.master_spark(), client=client),
        master_web_ui_url=SparkDiscovery.getOption(
            discovery.master_webui(), client=client),
        master_rest_endpoint=SparkDiscovery.getOption(
            discovery.master_rest(), client=client),
        operation_id=SparkDiscovery.getOption(
            discovery.operation(), client=client),
        shs_url=SparkDiscovery.getOption(discovery.shs(), client=client),
        spark_cluster_version=SparkDiscovery.getOption(
            discovery.spark_cluster_version(), client=client),
        children_operation_ids=SparkDiscovery.getOptions(
            discovery.children_operations(), client=client)
    )
