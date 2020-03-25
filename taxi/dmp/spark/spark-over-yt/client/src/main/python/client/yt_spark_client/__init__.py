import os
import re
import subprocess
from copy import deepcopy
from yt.wrapper import get
from yt.wrapper.cypress_commands import exists
from yt.wrapper.http_helpers import get_token, get_user_name, get_proxy_url
from yt.wrapper.operation_commands import TimeWatcher, process_operation_unsuccesful_finish_state
from yt.wrapper.run_operation_commands import run_operation
from yt.wrapper.spec_builders import VanillaSpecBuilder

from .utils import get_spark_master, base_spark_conf, SparkDiscovery, determine_cluster, SparkCluster


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
    return get(dynamic_config_path, client=client)


def build_spark_operation_spec(operation_alias, spark_discovery, dynamic_config,
                               worker_cores, worker_memory, worker_num, worker_timeout,
                               tmpfs_limit, master_memory_limit, history_server_memory_limit,
                               pool, operation_spec, client):
    def _launcher_command(component):
        unpack_tar = "tar --warning=no-unknown-keyword -xf spark.tgz -C ./tmpfs"
        move_java = "cp -r /opt/jdk8 ./tmpfs/jdk8"
        run_launcher = "./tmpfs/jdk8/bin/java -Xmx512m -cp spark-yt-launcher.jar"
        spark_conf = []
        for key, value in dynamic_config["spark_conf"].items():
            spark_conf.append("-D{}={}".format(key, value))
        spark_conf = " ".join(spark_conf)

        return "{0} && {1} && {2} {3} ru.yandex.spark.launcher.{4}Launcher ".format(unpack_tar, move_java, run_launcher,
                                                                                    spark_conf, component)

    master_command = _launcher_command("Master")
    worker_command = _launcher_command("Worker") + \
        "--cores {0} --memory {1} --wait-master-timeout {2}".format(worker_cores, worker_memory, worker_timeout)
    history_command = _launcher_command("HistoryServer") + "--log-path yt:/{}".format(spark_discovery.event_log())

    environment = dynamic_config["environment"]
    environment["YT_PROXY"] = get_proxy_url(required=True, client=client)
    environment["SPARK_DISCOVERY_PATH"] = str(spark_discovery.discovery())
    environment["JAVA_HOME"] = "$HOME/tmpfs/jdk8"
    environment["SPARK_HOME"] = "$HOME/tmpfs/spark"

    user = get_user_name(client=client)

    operation_spec = deepcopy(operation_spec or {})
    operation_spec["stderr_table_path"] = str(spark_discovery.stderr())
    operation_spec["pool"] = pool
    if "title" not in operation_spec:
        operation_spec["title"] = operation_alias or "spark_{}".format(user)

    common_task_spec = {
        "restart_completed_jobs": True,
        "file_paths": dynamic_config["file_paths"],
        "layer_paths": dynamic_config["layer_paths"],
        "environment": environment,
        "memory_reserve_factor": 1.0,
        "tmpfs_path": "tmpfs"
    }

    secure_vault = {"YT_USER": user, "YT_TOKEN": get_token(client=client)}

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
        .cpu_limit(worker_cores + 2) \
        .spec(common_task_spec) \
        .end_task() \
        .secure_vault(secure_vault) \
        .spec(operation_spec)


def start_spark_cluster(worker_cores, worker_memory, worker_num, worker_timeout="5m",
                        operation_alias=None, discovery_path=None, pool=None,
                        tmpfs_limit="150G", master_memory_limit="2G", history_server_memory_limit="8G",
                        dynamic_config_path="//sys/spark/conf/releases/spark-launch-conf",
                        operation_spec=None, client=None):
    """Start Spark cluster
    :param operation_alias: alias for the underlying YT operation
    :param pool: pool for the underlying YT operation
    :param discovery_path: Cypress path for discovery files and logs
    :param worker_cores: number of cores that will be available on worker
    :param worker_memory: amount of memory that will be available on worker
    :param worker_num: number of workers
    :param worker_timeout_minutes: timeout to fail master waiting
    :param tmpfs_limit: limit of tmpfs usage, default 150G
    :param master_memory_limit: memory limit for master, default 2G
    :param history_server_memory_limit: memory limit for history server, default 8G
    :param dynamic_config_path: YT path of dynamic config
    :param operation_spec: YT operation spec
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
                                              worker_timeout=worker_timeout,
                                              tmpfs_limit=tmpfs_limit,
                                              master_memory_limit=master_memory_limit,
                                              history_server_memory_limit=history_server_memory_limit,
                                              pool=pool,
                                              operation_spec=operation_spec,
                                              client=client)

    spark_discovery.create(client)
    op = run_operation(spec_builder, sync=False, client=client)
    _wait_master_start(op, spark_discovery, client)
    master_address = SparkDiscovery.get(spark_discovery.master_webui(), client=client)
    print("Spark Master's Web UI: http://{0}".format(master_address))

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
        shs_url=SparkDiscovery.get(discovery.shs(), client=client)
    )
