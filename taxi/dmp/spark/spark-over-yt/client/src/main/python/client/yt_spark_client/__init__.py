import json
import os
import re
import subprocess
from yt.wrapper import YPath
from yt.wrapper.cypress_commands import exists
from yt.wrapper.file_commands import read_file
from yt.wrapper.http_helpers import get_token, get_user_name
from yt.wrapper.operation_commands import TimeWatcher, process_operation_unsuccesful_finish_state
from yt.wrapper.run_operation_commands import run_operation
from yt.wrapper.spec_builders import VanillaSpecBuilder

from .utils import get_spark_master, base_spark_conf, SparkDiscovery, determine_cluster


def _add_conf(spark_conf, spark_args):
    if spark_conf:
        for k, v in spark_conf.items():
            spark_args.append("--conf")
            spark_args.append("{}={}".format(k, v))


def _add_master(discovery, spark_args, rest, client=None):
    spark_args.append("--master")
    spark_args.append(get_spark_master(discovery, rest, yt_client=client))


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
    spark_env["SPARK_USER"] = yt_user
    spark_env["YT_TOKEN"] = yt_token
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


def raw_submit(discovery_path, spark_home, spark_args, client=None, spark_id=None):
    spark_base_args = ["/usr/local/bin/spark-submit"]
    discovery = SparkDiscovery(discovery_path=discovery_path, spark_id=spark_id)
    _add_master(discovery, spark_base_args, rest=True, client=client)
    _add_base_spark_conf(client, discovery, spark_base_args)
    spark_env = _create_spark_env(client, spark_home)

    # replace stdin to avoid https://bugs.openjdk.java.net/browse/JDK-8211842
    subprocess.call(spark_base_args + spark_args, env=spark_env, stdin=subprocess.PIPE)


def shell(discovery_path, spark_home, spark_args, client=None, spark_id=None):
    spark_base_args = ["/usr/local/bin/spark-shell"]
    discovery = SparkDiscovery(discovery_path=discovery_path, spark_id=spark_id)
    _add_master(discovery, spark_base_args, rest=False, client=client)
    _add_base_spark_conf(client, discovery, spark_base_args)
    spark_env = _create_spark_env(client, spark_home)

    os.execve("/usr/local/bin/spark-shell", spark_base_args + spark_args, spark_env)


def _read_launch_config(dynamic_config_path, client=None):
    path = dynamic_config_path or "//sys/spark/bin/releases/spark-launch.json"
    return json.load(read_file(path, client=client))


def _launcher_command(component, config, opts):
    unpack_tar = "tar --warning=no-unknown-keyword -xf {0}.tgz".format(config["spark_name"])
    run_launcher = "/opt/jdk8/bin/java -Xmx512m -cp {0}".format(config["spark_launcher_name"])

    return "{0} && {1} ru.yandex.spark.launcher.{2}Launcher --port {3} --opts \"'{4}'\" " \
        .format(unpack_tar, run_launcher, component, config["start_port"], opts)


def build_spark_operation_spec(operation_alias, spark_discovery, dynamic_config,
                               worker_cores, worker_memory, worker_num,
                               tmpfs_limit, master_memory_limit, history_server_memory_limit,
                               pool, client):
    yt_proxy = determine_cluster(client)
    if "hahn" in yt_proxy or "arnold" in yt_proxy:
        proxy_role_opt = "-Dspark.hadoop.yt.proxyRole=spark "
    else:
        proxy_role_opt = ""
    common_ops = proxy_role_opt + \
                 "-Dspark.hadoop.fs.yt.impl=ru.yandex.spark.yt.fs.YtFileSystem " \
                 "-Dspark.port.maxRetries={0} " \
                 "-Dspark.shuffle.service.port={1} " \
                     .format(dynamic_config["port_max_retries"], dynamic_config["shuffle_service_port"])

    worker_opts = "-Dspark.worker.cleanup.enabled=true -Dspark.shuffle.service.enabled=true " + common_ops

    history_opts = "-Dspark.history.fs.cleaner.enabled=true " + common_ops

    master_opts = "-Dspark.master.rest.enabled=true " \
                  "-Dspark.master.rest.port={0} ".format(dynamic_config["start_port"]) + common_ops

    spark_yt_base_path = YPath(dynamic_config["spark_yt_base_path"])
    file_paths = [spark_yt_base_path.join(dynamic_config["spark_name"] + ".tgz"),
                  spark_yt_base_path.join(dynamic_config["spark_launcher_name"])]

    layer_paths = ["//home/sashbel/delta/jdk/layer_with_jdk_lastest.tar.gz",
                   "//home/sashbel/delta/python/layer_with_python37.tar.gz",
                   "//porto_layers/base/xenial/porto_layer_search_ubuntu_xenial_app_lastest.tar.gz"]

    master_command = _launcher_command("Master", dynamic_config, master_opts) + \
                     "--operation-id $YT_OPERATION_ID --web-ui-port {}".format(dynamic_config["start_port"])

    worker_command = _launcher_command("Worker", dynamic_config, worker_opts) + \
                     "--cores {0} --memory {1} --web-ui-port {2}".format(worker_cores, worker_memory,
                                                                         dynamic_config["start_port"])

    history_command = _launcher_command("HistoryServer", dynamic_config, history_opts) + \
                      "--log-path yt:/{}".format(spark_discovery.event_log())

    environment = {
        "JAVA_HOME": "/opt/jdk8",
        "SPARK_HOME": dynamic_config["spark_name"],
        "YT_PROXY": yt_proxy,
        "SPARK_DISCOVERY_PATH": str(spark_discovery.discovery()),
        "IS_SPARK_CLUSTER": "true"
    }

    operation_spec = {
        "stderr_table_path": str(spark_discovery.stderr()),
        "pool": pool,
        "annotations": {
            "is_spark": True
        }
    }

    task_spec = {
        "restart_completed_jobs": True
    }

    user = get_user_name(client=client)
    secure_vault = {
        "YT_USER": user,
        "YT_TOKEN": get_token(client=client)
    }

    master_memory_limit = master_memory_limit or dynamic_config.get("master_memory_limit") or "2G"
    history_server_memory_limit = history_server_memory_limit or dynamic_config.get(
        "history_server_memory_limit") or "8G"
    tmpfs_limit = tmpfs_limit or dynamic_config.get("tmpfs_limit") or "100G"

    return VanillaSpecBuilder() \
        .begin_task("master") \
        .job_count(1) \
        .file_paths(file_paths) \
        .command(master_command) \
        .memory_limit(_parse_memory(master_memory_limit)) \
        .memory_reserve_factor(1.0) \
        .cpu_limit(2) \
        .environment(environment) \
        .layer_paths(layer_paths) \
        .spec(task_spec) \
        .end_task() \
        .begin_task("history") \
        .job_count(1) \
        .file_paths(file_paths) \
        .command(history_command) \
        .memory_limit(_parse_memory(history_server_memory_limit)) \
        .memory_reserve_factor(1.0) \
        .cpu_limit(1) \
        .environment(environment) \
        .layer_paths(layer_paths) \
        .spec(task_spec) \
        .end_task() \
        .begin_task("workers") \
        .job_count(worker_num) \
        .file_paths(file_paths) \
        .command(worker_command) \
        .memory_limit(_parse_memory(worker_memory) + _parse_memory(tmpfs_limit)) \
        .memory_reserve_factor(1.0) \
        .cpu_limit(worker_cores + 2) \
        .environment(environment) \
        .layer_paths(layer_paths) \
        .spec(task_spec) \
        .tmpfs_path("tmpfs") \
        .end_task() \
        .secure_vault(secure_vault) \
        .max_failed_job_count(5) \
        .max_stderr_count(150) \
        .title(operation_alias or "spark_{}".format(user)) \
        .spec(operation_spec)


def start_spark_cluster(worker_cores, worker_memory, worker_num,
                        operation_alias=None, discovery_path=None, pool=None,
                        tmpfs_limit=None, master_memory_limit=None, history_server_memory_limit=None,
                        dynamic_config_path=None, client=None):
    """Start Spark cluster
    :param operation_alias: alias for the underlying YT operation
    :param pool: pool for the underlying YT operation
    :param discovery_path: Cypress path for discovery files and logs
    :param worker_cores: number of cores that will be available on worker
    :param worker_memory: amount of memory that will be available on worker
    :param worker_num: number of workers
    :param tmpfs_limit: limit of tmpfs usage, default 150G
    :param master_memory_limit: memory limit for master, default 2G
    :param history_server_memory_limit: memory limit for history server, default 8G
    :param dynamic_config_path: YT path of dynamic config
    :param client: YtClient
    :return:
    """
    spark_discovery = SparkDiscovery(discovery_path=discovery_path)
    dynamic_config = _read_launch_config(dynamic_config_path=dynamic_config_path, client=client)

    spec_builder = build_spark_operation_spec(operation_alias=operation_alias,
                                              spark_discovery=spark_discovery,
                                              dynamic_config=dynamic_config,
                                              worker_cores=worker_cores,
                                              worker_memory=worker_memory,
                                              worker_num=worker_num,
                                              tmpfs_limit=tmpfs_limit,
                                              master_memory_limit=master_memory_limit,
                                              history_server_memory_limit=history_server_memory_limit,
                                              pool=pool,
                                              client=client)

    spark_discovery.create(client)
    op = run_operation(spec_builder, sync=False, client=client)
    _wait_master_start(op, spark_discovery, client)
    master_address = SparkDiscovery.get(spark_discovery.master_webui(), client=client)
    print("Spark Master's Web UI: http://{0}".format(master_address))

    return op
