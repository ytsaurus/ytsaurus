import logging
import os
import re
import subprocess

from spyt.dependency_utils import require_yt_client

require_yt_client()

from yt.wrapper.cypress_commands import exists, copy as cypress_copy  # noqa: E402
from yt.wrapper.acl_commands import check_permission  # noqa: E402
from yt.wrapper.file_commands import upload_file_to_cache  # noqa: E402
from yt.wrapper.http_helpers import get_token, get_user_name  # noqa: E402
from yt.wrapper.operation_commands import TimeWatcher, \
    abort_operation, get_operation_state  # noqa: E402
from yt.wrapper.run_operation_commands import run_operation  # noqa: E402

try:
    from yt.wrapper.operation_commands import process_operation_unsuccessful_finish_state
except Exception:
    # COMPAT(alex-shishkin): For ytsaurus-client <= 0.13.7
    from yt.wrapper.operation_commands \
        import process_operation_unsuccesful_finish_state as process_operation_unsuccessful_finish_state

from .conf import read_remote_conf, validate_cluster_version, \
    latest_compatible_spyt_version, update_config_inplace, validate_custom_params, validate_mtn_config, \
    latest_ytserver_proxy_path, read_global_conf, python_bin_path, \
    worker_num_limit, validate_worker_num, read_cluster_conf, validate_ssd_config  # noqa: E402
from .utils import get_spark_master, base_spark_conf, SparkDiscovery, SparkCluster, call_get_proxy_address_url, \
    parse_bool  # noqa: E402
from .enabler import SpytEnablers  # noqa: E402
from .spec import SparkDefaultArguments, CommonComponentConfig, MasterConfig, WorkerConfig, HistoryServerConfig, \
    LivyConfig, build_spark_operation_spec, WorkerResources  # noqa: E402
from .version import __scala_version__  # noqa: E402
from .utils import get_spyt_home  # noqa: E402

logger = logging.getLogger(__name__)


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
    yt_proxy = call_get_proxy_address_url(client=client)
    spark_env["SPARK_USER"] = yt_user
    spark_env["SPARK_YT_TOKEN"] = yt_token
    spark_env["SPARK_YT_PROXY"] = yt_proxy
    spark_env["SPARK_CONF_DIR"] = os.path.join(get_spyt_home(), "conf")
    if spark_home:
        spark_env["SPARK_HOME"] = spark_home
    return spark_env


def _wait_op_start(op, operation_path=None, client=None):
    for state in op.get_state_monitor(TimeWatcher(5.0, 5.0, 0.0)):
        if state.is_running() and operation_path is not None and exists(operation_path, client=client):
            return op
        elif state.is_unsuccessfully_finished():
            process_operation_unsuccessful_finish_state(op, op.get_error(state))
        else:
            op.printer(state)


def _wait_child_start(op, spark_discovery, client):
    _wait_op_start(op, spark_discovery.children_operations().join(op.id), client)


def _wait_master_start(op, spark_discovery, client):
    _wait_op_start(op, spark_discovery.operation().join(op.id), client)


def _wait_livy_start(op, spark_discovery, client):
    _wait_op_start(op, spark_discovery.livy(), client)


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


def raw_submit(discovery_path, spark_home, spark_args,
               python_version=None, local_files=True, client=None):
    spark_submit_path = "{}/bin/spark-submit".format(spark_home)
    spark_base_args = [spark_submit_path]
    permission_status = check_permission(user=client.get_user_name(),
                                         permission='read', path=discovery_path, client=client)
    if permission_status.get('action', 'deny') != 'allow':
        raise RuntimeError(
            'No permission for reading cluster, actual permission status is ' + str(permission_status))
    discovery = SparkDiscovery(discovery_path=discovery_path)

    cluster_conf = read_cluster_conf(str(discovery.conf()), client)
    spark_conf = cluster_conf['spark_conf']
    dedicated_driver_op = parse_bool(spark_conf.get('spark.dedicated_operation_mode'))
    ipv6_preference_enabled = parse_bool(spark_conf.get('spark.hadoop.yt.preferenceIpv6.enabled'))

    _add_master(discovery, spark_base_args, rest=True, client=client)
    _add_shs_option(discovery, spark_base_args, client=client)
    _add_base_spark_conf(client, discovery, spark_base_args)
    _add_python_version(python_version, spark_base_args, client)
    _add_dedicated_driver_op_conf(spark_base_args, dedicated_driver_op)
    _add_ipv6_preference(ipv6_preference_enabled, spark_base_args)
    spark_env = _create_spark_env(client, spark_home)

    if local_files:
        remote_paths = {}
        new_spark_args = []
        # Matches "FS:/..." path sequence
        local_files_pattern = "[A-Za-z0-9]+:\/[^,]+(,[A-Za-z0-9]+:\/[^,]+)*"  # noqa: W605
        for spark_arg in spark_args:
            is_file_list = re.fullmatch(local_files_pattern, spark_arg)
            if not is_file_list:
                new_spark_args.append(spark_arg)
                continue
            file_list = spark_arg.split(",")
            new_file_list = []
            for single_file in file_list:
                if single_file.startswith('local:/'):
                    local_file_path = single_file[7:]  # Drops prefix
                    if local_file_path not in remote_paths:
                        _, file_extension = os.path.splitext(local_file_path)
                        destination = upload_file_to_cache(local_file_path, client=client)
                        destination_ext = "{}{}".format(destination, file_extension)
                        if file_extension != "":
                            cypress_copy(destination, destination_ext, ignore_existing=True, client=client)  # Extension is necessary
                        else:
                            logger.warn("%s has no extension. Usually such files are unacceptable")
                        logger.info("%s has been uploaded to YT as %s", local_file_path, destination_ext)
                        remote_paths[local_file_path] = "yt:/{}".format(destination_ext)
                    new_file_list.append(remote_paths[local_file_path])
                else:
                    new_file_list.append(single_file)
            new_spark_arg = ",".join(new_file_list)
            new_spark_args.append(new_spark_arg)
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


def _add_ipv6_preference(ipv6_preference_enabled, spark_args):
    if ipv6_preference_enabled:
        _add_conf({
            'spark.driver.extraJavaOptions': '-Djava.net.preferIPv6Addresses=true',
            'spark.executor.extraJavaOptions': '-Djava.net.preferIPv6Addresses=true'
        }, spark_args)


def wrap_cached_jar(path, jar_caching_enabled):
    if jar_caching_enabled and path.startswith("yt:/"):
        return "ytCached:/" + path[4:]
    else:
        return path


def shell(discovery_path, spark_home, spark_args, spyt_version=None, client=None):
    spark_shell_path = "{}/bin/spark-shell".format(spark_home)
    spark_base_args = [spark_shell_path]
    discovery = SparkDiscovery(discovery_path=discovery_path)

    spark_conf = read_cluster_conf(str(discovery.conf()), client)['spark_conf']
    ipv6_preference_enabled = parse_bool(spark_conf.get('spark.hadoop.yt.preferenceIpv6.enabled'))

    _add_master(discovery, spark_base_args, rest=False, client=client)
    _add_shs_option(discovery, spark_base_args, client=client)
    _add_base_spark_conf(client, discovery, spark_base_args)
    _add_conf({"spark.ui.showConsoleProgress": "true"}, spark_base_args)
    _add_conf(spark_conf, spark_base_args)
    _add_ipv6_preference(ipv6_preference_enabled, spark_base_args)
    spark_env = _create_spark_env(client, spark_home)

    os.execve(spark_shell_path, spark_base_args + spark_args, spark_env)


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


def get_base_cluster_config(global_conf, spark_cluster_version, params, base_discovery_path=None, client=None):
    dynamic_config = SparkDefaultArguments.get_params()
    update_config_inplace(dynamic_config, read_remote_conf(global_conf, spark_cluster_version, client=client))
    update_config_inplace(dynamic_config, params)
    if base_discovery_path is not None:
        dynamic_config['spark_conf']['spark.base.discovery.path'] = base_discovery_path
    return dynamic_config


def start_livy_server(operation_alias=None, discovery_path=None, pool=None, enable_tmpfs=True, network_project=None,
                      tvm_id=None, tvm_secret=None, params=None, spark_cluster_version=None, enablers=None, client=None,
                      preemption_mode="normal", cluster_log_level="INFO",
                      livy_driver_cores=SparkDefaultArguments.LIVY_DRIVER_CORES,
                      livy_driver_memory=SparkDefaultArguments.LIVY_DRIVER_MEMORY,
                      livy_max_sessions=SparkDefaultArguments.LIVY_MAX_SESSIONS, spark_master_address=None,
                      rpc_job_proxy=False, rpc_job_proxy_thread_pool_size=4, tcp_proxy_range_start=30000,
                      tcp_proxy_range_size=100, enable_stderr_table=False, master_group_id=None, group_id=None):
    if discovery_path is None and group_id is None:
        raise RuntimeError("Either discovery path or discovery group id must be provided")

    enablers = enablers or SpytEnablers()

    if spark_master_address is None:
        logger.warning("Spark master address is not specified, "
                       "standalone cluster will be discovered. "
                       "If you want use direct submit, "
                       "please provide the option `--spark-master-address ytsaurus://<ytsaurus-proxy>`")

    if spark_cluster_version is None:
        spark_cluster_version = latest_compatible_spyt_version(__scala_version__, client=client)

    validate_cluster_version(spark_cluster_version, client=client)
    validate_custom_params(params)
    validate_mtn_config(enablers, network_project, tvm_id, tvm_secret)

    global_conf = read_global_conf(client=client)
    dynamic_config = get_base_cluster_config(global_conf, spark_cluster_version, params, discovery_path, client)
    enablers.apply_config(dynamic_config)

    spark_discovery = None
    if discovery_path is not None:
        spark_discovery = SparkDiscovery(discovery_path=discovery_path)
        spark_discovery.create(client)
        spark_discovery.set_cluster_version_if_none(spark_cluster_version, client)

    common_config = CommonComponentConfig(
        operation_alias, pool, enable_tmpfs, network_project, tvm_id, tvm_secret, enablers, preemption_mode,
        cluster_log_level, rpc_job_proxy, rpc_job_proxy_thread_pool_size, tcp_proxy_range_start,
        tcp_proxy_range_size, enable_stderr_table, "livy", spark_discovery, group_id
    )
    livy_config = LivyConfig(
        livy_driver_cores, livy_driver_memory, livy_max_sessions, spark_master_address, master_group_id
    )
    livy_args = {
        'config': dynamic_config,
        'client': client,
        'job_types': ['livy'],
        'common_config': common_config,
        'livy_config': livy_config,
    }
    livy_builder = build_spark_operation_spec(**livy_args)

    op = None
    try:
        op = run_operation(livy_builder, sync=False, client=client)
        if spark_discovery is not None:
            _wait_livy_start(op, spark_discovery, client)
            logger.info("Livy operation %s", op.id)
            livy_address = SparkDiscovery.get(spark_discovery.livy(), client=client)
            logger.info("Livy UI: http://{0}".format(livy_address))
        return op
    except Exception as err:
        logging.error(err, exc_info=True)
        abort_operation_silently(op, client)


def start_spark_cluster(worker_cores, worker_memory, worker_num, worker_cores_overhead=None,
                        worker_memory_overhead=SparkDefaultArguments.SPARK_WORKER_MEMORY_OVERHEAD,
                        worker_timeout=SparkDefaultArguments.SPARK_WORKER_TIMEOUT,
                        operation_alias=None, discovery_path=None, pool=None,
                        enable_tmpfs=True, tmpfs_limit=SparkDefaultArguments.SPARK_WORKER_TMPFS_LIMIT,
                        ssd_limit=SparkDefaultArguments.SPARK_WORKER_SSD_LIMIT,
                        ssd_account=None, worker_disk_name="default", worker_disk_limit=None, worker_disk_account=None,
                        worker_port=SparkDefaultArguments.SPARK_WORKER_PORT,
                        master_memory_limit=SparkDefaultArguments.SPARK_MASTER_MEMORY_LIMIT,
                        master_port=SparkDefaultArguments.SPARK_MASTER_PORT,
                        enable_history_server=True,
                        history_server_memory_limit=SparkDefaultArguments.SPARK_HISTORY_SERVER_MEMORY_LIMIT,
                        history_server_cpu_limit=SparkDefaultArguments.SPARK_HISTORY_SERVER_CPU_LIMIT,
                        history_server_memory_overhead=SparkDefaultArguments.SPARK_HISTORY_SERVER_MEMORY_OVERHEAD,
                        network_project=None, abort_existing=False, tvm_id=None, tvm_secret=None,
                        advanced_event_log=False, worker_log_transfer=False, worker_log_json_mode=False,
                        worker_log_update_interval=SparkDefaultArguments.SPARK_WORKER_LOG_UPDATE_INTERVAL,
                        worker_log_table_ttl=SparkDefaultArguments.SPARK_WORKER_LOG_TABLE_TTL,
                        params=None, shs_location=None, spark_cluster_version=None, enablers=None, client=None,
                        preemption_mode="normal", cluster_log_level="INFO", enable_multi_operation_mode=False,
                        dedicated_operation_mode=False, driver_cores=None, driver_memory=None, driver_num=None,
                        driver_cores_overhead=None, driver_timeout=None, autoscaler_period=None,
                        autoscaler_metrics_port=None, autoscaler_sliding_window=None,
                        autoscaler_max_free_workers=None, autoscaler_slot_increment_step=None,
                        enable_livy=False, livy_driver_cores=SparkDefaultArguments.LIVY_DRIVER_CORES,
                        livy_driver_memory=SparkDefaultArguments.LIVY_DRIVER_MEMORY,
                        livy_max_sessions=SparkDefaultArguments.LIVY_MAX_SESSIONS, rpc_job_proxy=False,
                        rpc_job_proxy_thread_pool_size=4, tcp_proxy_range_start=30000,
                        tcp_proxy_range_size=100, enable_stderr_table=False, group_id=None):
    """Start Spark cluster
    :param operation_alias: alias for the underlying YT operation
    :param pool: pool for the underlying YT operation
    :param discovery_path: Cypress path for discovery files and logs
    :param worker_cores: number of cores that will be available on worker
    :param worker_memory: amount of memory that will be available on worker
    :param worker_num: number of workers
    :param worker_cores_overhead: additional worker cores
    :param worker_memory_overhead: additional worker memory
    :param worker_timeout: timeout to fail master waiting
    :param worker_port: starting port to bind worker rpc endpoint
    :param enable_tmpfs: mounting ram memory as directory 'tmpfs'
    :param tmpfs_limit: limit of tmpfs usage, default 150G
    :param ssd_limit: limit of ssd usage, default None, ssd disabled
    :param ssd_account: account for ssd quota
    :param worker_disk_name: medium name
    :param worker_disk_limit: limit of disk usage, default None, disk disabled
    :param worker_disk_account: account for disk quota
    :param master_memory_limit: memory limit for master, default 2G
    :param master_port: starting port to bind master rpc endpoint
    :param enable_history_server: enables SHS
    :param history_server_memory_limit: memory limit for history server, default 16G,
    total memory for SHS job is history_server_memory_limit + history_server_memory_overhead
    :param history_server_cpu_limit: cpu limit for history server, default 20
    :param history_server_memory_overhead: memory overhead for history server, default 2G,
    total memory for SHS job is history_server_memory_limit + history_server_memory_overhead
    :param spark_cluster_version: SPYT cluster version
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
    :param cluster_log_level: level for cluster logs
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
    :param enable_livy: start livy server
    :param livy_driver_cores: core limit for livy drivers
    :param livy_driver_memory: memory limit for livy drivers
    :param livy_max_sessions: session count limit for livy server
    :param rpc_job_proxy: using RPC proxy in job proxy
    :param rpc_job_proxy_thread_pool_size: RPC proxy thread pool size
    :param tcp_proxy_range_start: start port of TCP proxy allocation range
    :param tcp_proxy_range_size: size of TCP proxy allocation range
    :param enable_stderr_table: enables writing YT operation logs to stderr table
    :param group_id: discovery group id
    :return:
    """
    worker_res = WorkerResources(worker_cores, worker_memory, worker_num, worker_cores_overhead, worker_timeout,
                                 worker_memory_overhead)
    driver_res = WorkerResources(driver_cores or worker_cores, driver_memory or worker_memory, driver_num or worker_num,
                                 driver_cores_overhead or worker_cores_overhead, driver_timeout or worker_timeout,
                                 worker_memory_overhead)
    dedicated_operation_mode = dedicated_operation_mode and driver_num > 0

    enablers = enablers or SpytEnablers()

    spark_discovery = SparkDiscovery(discovery_path=discovery_path)

    current_operation_id = SparkDiscovery.getOption(spark_discovery.operation(), client=client)
    if current_operation_id is not None and get_operation_state(current_operation_id, client=client).is_running():
        if abort_existing:
            abort_spark_operations(spark_discovery, client)
        else:
            raise RuntimeError("This spark cluster is started already, use --abort-existing for auto restarting")

    ytserver_proxy_path = latest_ytserver_proxy_path(spark_cluster_version, client=client)
    global_conf = read_global_conf(client=client)
    if spark_cluster_version is None:
        spark_cluster_version = latest_compatible_spyt_version(__scala_version__, client=client)
    logger.info(f"{spark_cluster_version} cluster version will be launched")

    if ssd_limit is not None:
        worker_disk_name = "ssd_slots_physical"
        worker_disk_limit = ssd_limit
        worker_disk_account = ssd_account

    if enable_tmpfs:
        logger.info("Tmpfs is enabled, spills will be created at RAM")

    if worker_disk_limit is None:
        logger.info("No disk account is specified")
        if enable_tmpfs:
            logger.info("Launcher files will be placed to tmpfs")
        else:
            logger.info("Launcher files will be placed to node disk with no guarantees on free space")

    validate_cluster_version(spark_cluster_version, client=client)
    validate_custom_params(params)
    validate_mtn_config(enablers, network_project, tvm_id, tvm_secret)
    validate_worker_num(worker_res.num, worker_num_limit(global_conf))
    validate_ssd_config(worker_disk_limit, worker_disk_account)

    dynamic_config = get_base_cluster_config(global_conf, spark_cluster_version, params,
                                             spark_discovery.base_discovery_path, client)
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

    enablers.apply_config(dynamic_config)

    spark_discovery.create(client)

    job_types = ['master']
    if enable_history_server:
        job_types.append('history')
    if enable_livy:
        job_types.append('livy')
    if not enable_multi_operation_mode:
        job_types.append('worker')

    common_config = CommonComponentConfig(
        operation_alias, pool, enable_tmpfs, network_project, tvm_id, tvm_secret, enablers, preemption_mode,
        cluster_log_level, rpc_job_proxy, rpc_job_proxy_thread_pool_size, tcp_proxy_range_start,
        tcp_proxy_range_size, enable_stderr_table, "spark", spark_discovery, group_id
    )
    master_config = MasterConfig(master_memory_limit, master_port)
    worker_config = WorkerConfig(
        tmpfs_limit, worker_res, worker_port, None, True, autoscaler_period is not None, worker_log_transfer,
        worker_log_json_mode, worker_log_update_interval, worker_log_table_ttl, worker_disk_name
    )
    hs_config = HistoryServerConfig(
        history_server_memory_limit, history_server_cpu_limit, history_server_memory_overhead, shs_location,
        advanced_event_log
    )
    livy_config = LivyConfig(livy_driver_cores, livy_driver_memory, livy_max_sessions)
    args = {
        'config': dynamic_config,
        'client': client,
        'job_types': job_types,
        'common_config': common_config,
        'master_config': master_config,
        'worker_config': worker_config,
        'hs_config': hs_config,
        'livy_config': livy_config,
    }

    master_args = args.copy()
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
            driver_config = worker_config._replace(res=driver_res,
                                                   driver_op_discovery_script='spark/bin/driver-op-discovery.sh')
            driver_args['worker_config'] = driver_config
            driver_builder = build_spark_operation_spec(**driver_args)
            op_driver = run_operation(driver_builder, sync=False, client=client)
            _wait_child_start(op_driver, spark_discovery, client)
            logger.info("Driver operation %s", op_driver.id)

        master_address = SparkDiscovery.get(spark_discovery.master_webui(), client=client)
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
        master_endpoint=SparkDiscovery.getOption(discovery.master_spark(), client=client),
        master_web_ui_url=SparkDiscovery.getOption(discovery.master_webui(), client=client),
        master_rest_endpoint=SparkDiscovery.getOption(discovery.master_rest(), client=client),
        operation_id=SparkDiscovery.getOption(discovery.operation(), client=client),
        shs_url=SparkDiscovery.getOption(discovery.shs(), client=client),
        livy_url=SparkDiscovery.getOption(discovery.livy(), client=client),
        spark_cluster_version=SparkDiscovery.getOption(discovery.spark_cluster_version(), client=client),
        children_operation_ids=SparkDiscovery.getOptions(discovery.children_operations(), client=client)
    )
