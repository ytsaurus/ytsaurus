import os
import re
import subprocess
import yaml

import yt
from yt.wrapper.cypress_commands import list as yt_list, create, exists
from yt.wrapper.operation_commands import TimeWatcher, process_operation_unsuccesful_finish_state
from yt.wrapper.run_operation_commands import run_operation
from yt.wrapper.spec_builders import VanillaSpecBuilder


def _yt_client(yt_proxy, yt_token):
    return yt.wrapper.YtClient(proxy=yt_proxy, token=yt_token)


def _add_conf(spark_conf, spark_args):
    if spark_conf:
        for k, v in spark_conf.items():
            spark_args.append("--conf")
            spark_args.append("{}={}".format(k, v))


def _add_master(spark_id, yt_proxy, discovery_dir, spark_args, rest, client):
    yt.wrapper.config.config['proxy']['url'] = yt_proxy
    master_path = "rest" if rest else "address"
    master = yt_list("{0}/instances/{1}/{2}".format(discovery_dir, spark_id, master_path), client=client)[0]
    spark_args.append("--master")
    spark_args.append("spark://{0}".format(master))


def _add_base_spark_conf(yt_proxy, yt_user, log_dir, spark_args):
    spark_conf = {
        "spark.hadoop.yt.proxy": yt_proxy,
        "spark.hadoop.yt.user": yt_user,
        "spark.master.rest.enabled": "true",
        "spark.eventLog.dir": "yt:/{}".format(log_dir),
    }
    _add_conf(spark_conf, spark_args)


def _add_job_args(job_args, spark_args):
    if job_args:
        for k, v in job_args.items():
            spark_args.append("--{}".format(k))
            spark_args.append(v)


def _create_spark_env(yt_user, yt_token, spark_home):
    spark_env = os.environ.copy()
    spark_env["SPARK_USER"] = yt_user
    spark_env["YT_TOKEN"] = yt_token
    spark_env["SPARK_HOME"] = spark_home
    return spark_env


def _parse_memory(memory_str):
    units = {"gb": 1024 * 1024 * 1024, "mb": 1024 * 1024, "kb": 1024, "bb": 1, "b": 1}
    if memory_str is None:
        return None
    m = re.match("(\d+)(.*)", memory_str)
    value = long(m.group(1))
    unit = m.group(2).lower().strip()
    if len(unit) <= 1:
        unit = unit + "b"
    return value * units[unit]


def default_token():
    with open("{}/.yt/token".format(os.getenv("HOME"))) as f:
        token = f.readline().strip()
    return token


def default_discovery_dir():
    return os.getenv("SPARK_YT_DISCOVERY_DIR") or "//home/{0}/spark-tmp".format(os.getenv("USER"))


def default_base_log_dir(discovery_dir):
    return os.getenv("SPARK_YT_LOG_DIR") or "{}/logs".format(discovery_dir)


def _wait_master_start(op, spark_id, discovery_dir, client):
    operation_path = "{0}/instances/{1}/operation/{2}".format(discovery_dir, spark_id, op.id)
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


def submit(spark_id, discovery_dir, log_dir, yt_proxy, yt_user, yt_token, spark_home,
           deploy_mode, spark_conf, job_class, jar_path, job_args):
    spark_args = ["--deploy-mode", deploy_mode]

    _add_conf(spark_conf, spark_args)

    spark_args.append("--class")
    spark_args.append(job_class)
    spark_args.append(jar_path)

    _add_job_args(job_args, spark_args)

    raw_submit(spark_id, discovery_dir, log_dir, yt_proxy, yt_user, yt_token, spark_home, *spark_args)


def submit_python(spark_id, discovery_dir, log_dir, yt_proxy, yt_user, yt_token, spark_home,
                  deploy_mode, spark_conf, main_py_path, py_files, job_args):
    spark_args = ["--deploy-mode", deploy_mode]

    _add_conf(spark_conf, spark_args)

    spark_args.append("--py-files")
    spark_args.append(py_files)
    spark_args.append(main_py_path)

    _add_job_args(job_args, spark_args)

    raw_submit(spark_id, discovery_dir, log_dir, yt_proxy, yt_user, yt_token, spark_home, *spark_args)


def raw_submit(spark_id, discovery_dir, log_dir, yt_proxy, yt_user, yt_token, spark_home, *args):
    spark_base_args = ["/usr/local/bin/spark-submit"]
    _add_master(spark_id, yt_proxy, discovery_dir, spark_base_args, rest=True, client=_yt_client(yt_proxy, yt_token))
    _add_base_spark_conf(yt_proxy, yt_user, log_dir, spark_base_args)
    spark_env = _create_spark_env(yt_user, yt_token, spark_home)

    # replace stdin to avoid https://bugs.openjdk.java.net/browse/JDK-8211842
    subprocess.call(spark_base_args + list(args), env=spark_env, stdin=subprocess.PIPE)


def shell(spark_id, discovery_dir, log_dir, yt_proxy, yt_user, yt_token, spark_home, *args):
    spark_base_args = ["/usr/local/bin/spark-shell"]
    _add_master(spark_id, yt_proxy, discovery_dir, spark_base_args, rest=False, client=_yt_client(yt_proxy, yt_token))
    _add_base_spark_conf(yt_proxy, yt_user, log_dir, spark_base_args)
    spark_env = _create_spark_env(yt_user, yt_token, spark_home)

    os.execve("/usr/local/bin/spark-shell", spark_base_args + list(args), spark_env)


def launch(spark_id, discovery_dir, log_base_dir, yt_proxy, yt_user, yt_token, yt_pool,
           worker_cores, worker_memory, worker_num, master_memory_limit="2G"):
    with open("{}/conf/spark-launch.yaml".format(os.getenv("SPARK_HOME"))) as f:
        config = yaml.load(f, Loader=yaml.BaseLoader)

    recovery_opts = "-Dspark.deploy.recoveryMode=CUSTOM " \
                    "-Dspark.deploy.recoveryMode.factory=org.apache.spark.deploy.master.YtRecoveryModeFactory " \
                    "-Dspark.deploy.yt.path=/home/sashbel/master"

    worker_opts = "-Dspark.worker.cleanup.enabled=true " \
                  "-Dspark.shuffle.service.enabled=true " \
                  "-Dspark.hadoop.fs.yt.impl=ru.yandex.spark.yt.fs.YtFileSystem " \
                  "-Dspark.port.maxRetries={0} " \
                  "-Dspark.shuffle.service.port={1}" \
        .format(config["port_max_retries"], config["shuffle_service_port"])

    master_opts = "-Dspark.port.maxRetries={0} " \
                  "-Dspark.hadoop.fs.yt.impl=ru.yandex.spark.yt.fs.YtFileSystem " \
                  "-Dspark.master.rest.enabled=true " \
                  "-Dspark.master.rest.port={1}" \
        .format(config["port_max_retries"], config["start_port"])

    unpack_tar = "tar --warning=no-unknown-keyword -xf {0}.tgz".format(config["spark_name"])
    run_launcher = "/opt/jdk8/bin/java -Xmx512m -cp {0}".format(config["spark_launcher_name"])

    file_paths = ["{0}/{1}.tgz".format(config["spark_yt_base_path"], config["spark_name"]),
                  "{0}/{1}".format(config["spark_yt_base_path"], config["spark_launcher_name"])]

    layer_paths = ["//home/sashbel/delta/jdk/layer_with_jdk_lastest.tar.gz",
                   "//porto_layers/base/xenial/porto_layer_search_ubuntu_xenial_app_lastest.tar.gz"]

    master_command = "{0} && {1} ru.yandex.spark.launcher.MasterLauncher --id {2} --operation-id $YT_OPERATION_ID " \
                     "--port {3} --web-ui-port {3} --opts \"'{4}'\"" \
        .format(unpack_tar, run_launcher, spark_id, config["start_port"], master_opts)

    worker_command = "{0} && {1} ru.yandex.spark.launcher.WorkerLauncher --id {2} --cores {3} --memory {4} " \
                     "--port {5} --web-ui-port {5} --opts \"'{6}'\"" \
        .format(unpack_tar, run_launcher, spark_id, worker_cores, worker_memory,
                config["start_port"], worker_opts)

    log_dir = "{}/{}".format(log_base_dir, spark_id)
    history_command = "{0} && {1} ru.yandex.spark.launcher.HistoryServerLauncher --id {2} --log-path yt:/{3} " \
                      "--port {4} --opts \"'{5}'\"" \
        .format(unpack_tar, run_launcher, spark_id, log_dir, config["start_port"], worker_opts)

    environment = {
        "JAVA_HOME": "/opt/jdk8",
        "SPARK_HOME": config["spark_name"],
        "YT_PROXY": yt_proxy,
        "SPARK_DISCOVERY_PATH": "{0}/instances".format(discovery_dir)
    }

    operation_spec = {
        "stderr_table_path": "{0}/logs/stderr_{1}".format(discovery_dir, spark_id),
        "pool": yt_pool
    }

    task_spec = {
        "restart_completed_jobs": True
    }

    secure_vault = {
        "YT_USER": yt_user,
        "YT_TOKEN": yt_token
    }

    spec_builder = \
        VanillaSpecBuilder() \
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
                .memory_limit(_parse_memory("8G")) \
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
                .memory_limit(_parse_memory(worker_memory) + _parse_memory("100G")) \
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
            .spec(operation_spec)

    yt_client = _yt_client(yt_proxy, yt_token)
    create("map_node", "{0}/instances".format(discovery_dir), recursive=True, ignore_existing=True, client=yt_client)
    create("map_node", log_dir, recursive=True, ignore_existing=True, client=yt_client)
    op = run_operation(spec_builder, sync=False, client=yt_client)
    _wait_master_start(op, spark_id, discovery_dir, yt_client)
    master_address = yt_list("{0}/instances/{1}/webui".format(discovery_dir, spark_id), client=yt_client)[0]

    print("Spark Master's Web UI: http://{0}".format(master_address))
