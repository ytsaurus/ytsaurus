from __future__ import print_function

from .cypress_commands import list as yt_list, create, exists
from .http_helpers import get_token, get_user_name, get_proxy_url
from .operation_commands import TimeWatcher, process_operation_unsuccesful_finish_state
from .run_operation_commands import run_operation
from .spec_builders import VanillaSpecBuilder
from .ypath import YPath
from .file_commands import read_file
from .errors import YtHttpResponseError
from .operation_commands import get_operation_url
from .common import require

import json
import os
import re
import logging


class SparkCluster(object):
    def __init__(self, master_endpoint, master_web_ui_url, master_rest_endpoint, operation_id, shs_url):
        self.master_endpoint = master_endpoint
        self.master_web_ui_url = master_web_ui_url
        self.master_rest_endpoint = master_rest_endpoint
        self.operation_id = operation_id
        self.shs_url = shs_url

    def operation_url(self, client=None):
        return get_operation_url(self.operation_id, client=client)


class SparkDiscovery(object):
    def __init__(self, discovery_path=None):
        discovery_path = discovery_path or os.environ.get("SPARK_YT_DISCOVERY_PATH")
        require(discovery_path, "Discovery path argument is not set. Provide either python argument, or CLI argument, "
                                "or SPARK_YT_DISCOVERY_PATH environment variable")
        self.base_discovery_path = YPath(discovery_path)

    @staticmethod
    def get(path, client=None):
        try:
            return yt_list(path, client=client)[0]
        except YtHttpResponseError as err:
            logging.warning("Failed to list {}, error {}".format(path, err.message))
            for inner in err.inner_errors:
                logging.warning("Failed to list {}, inner error {}".format(path, inner["message"]))

    def create(self, client):
        create("map_node", self.discovery(), recursive=True, ignore_existing=True, client=client)
        create("map_node", self.event_log(), recursive=True, ignore_existing=True, client=client)

    def discovery(self):
        return self.base_discovery_path.join("discovery")

    def operation(self):
        return self.discovery().join("operation")

    def logs(self):
        return self.base_discovery_path.join("logs")

    def event_log(self):
        return self.logs().join("event_log")

    def master_spark(self):
        return self.discovery().join("spark_address")

    def master_webui(self):
        return self.discovery().join("webui")

    def master_rest(self):
        return self.discovery().join("rest")

    def shs(self):
        return self.discovery().join("shs")

    def stderr(self):
        return self.logs().join("stderr")


def _wait_master_start(op, spark_discovery, client):
    operation_path = spark_discovery.operation().join(op.id)
    for state in op.get_state_monitor(TimeWatcher(1.0, 1.0, 0.0)):
        if state.is_running() and exists(operation_path, client=client):
            return op
        elif state.is_unsuccessfully_finished():
            process_operation_unsuccesful_finish_state(op, state)
        else:
            op.printer(state)


def _format_memory(memory_bytes):
    units = {"gb": 1024 * 1024 * 1024, "mb": 1024 * 1024, "kb": 1024, "bb": 1, "b": 1}
    if memory_bytes % units["gb"] == 0:
        return "{}G".format(memory_bytes // units["gb"])
    if memory_bytes % units["mb"] == 0:
        return "{}M".format(memory_bytes // units["mb"])
    if memory_bytes % units["kb"] == 0:
        return "{}K".format(memory_bytes // units["kb"])
    return "{}B".format(memory_bytes)


def _read_launch_config(dynamic_config_path, client=None):
    path = dynamic_config_path or "//sys/spark/bin/releases/spark-launch.json"
    return json.loads(read_file(path, client=client).read().decode("utf-8"))


def _launcher_command(component, config, opts):
    unpack_tar = "tar --warning=no-unknown-keyword -xf {0}.tgz".format(config["spark_name"])
    run_launcher = "/opt/jdk8/bin/java -Xmx512m -cp {0}".format(config["spark_launcher_name"])

    return "{0} && {1} ru.yandex.spark.launcher.{2}Launcher --port {3} --opts \"'{4}'\" " \
        .format(unpack_tar, run_launcher, component, config["start_port"], opts)


def build_spark_operation_spec(operation_alias, spark_discovery, dynamic_config,
                               spark_worker_core_count, spark_worker_memory_limit, spark_worker_count,
                               spark_worker_tmpfs_limit, spark_master_memory_limit, spark_history_server_memory_limit,
                               pool, client=None):
    yt_proxy = get_proxy_url(client=client)
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

    master_command = _launcher_command("Master", dynamic_config, master_opts) + \
                     "--operation-id $YT_OPERATION_ID --web-ui-port {}".format(dynamic_config["start_port"])

    worker_command = _launcher_command("Worker", dynamic_config, worker_opts) + \
                     "--cores {0} --memory {1} --web-ui-port {2}".format(spark_worker_core_count,
                                                                         _format_memory(spark_worker_memory_limit),
                                                                         dynamic_config["start_port"])

    history_command = _launcher_command("HistoryServer", dynamic_config, history_opts) + \
                      "--log-path yt:/{}".format(spark_discovery.event_log())

    operation_spec = {
        "stderr_table_path": spark_discovery.stderr(),
        "pool": pool,
        "annotations": {
            "is_spark": True
        }
    }

    common_task_spec = {
        "restart_completed_jobs": True,
        "file_paths": [YPath(dynamic_config["spark_yt_base_path"]).join(dynamic_config["spark_name"] + ".tgz"),
                       YPath(dynamic_config["spark_yt_base_path"]).join(dynamic_config["spark_launcher_name"])],
        "layer_paths": ["//home/sashbel/delta/jdk/layer_with_jdk_lastest.tar.gz",
                        "//home/sashbel/delta/python/layer_with_python37.tar.gz",
                        "//porto_layers/base/xenial/porto_layer_search_ubuntu_xenial_app_lastest.tar.gz"],
        "environment": {
            "JAVA_HOME": "/opt/jdk8",
            "SPARK_HOME": dynamic_config["spark_name"],
            "YT_PROXY": yt_proxy,
            "SPARK_DISCOVERY_PATH": spark_discovery.discovery(),
            "IS_SPARK_CLUSTER": "true"
        },
        "memory_reserve_factor": 1.0
    }

    user = get_user_name(client=client)
    secure_vault = {"YT_USER": user, "YT_TOKEN": get_token(client=client)}

    return VanillaSpecBuilder() \
        .begin_task("master") \
        .job_count(1) \
        .command(master_command) \
        .memory_limit(spark_master_memory_limit) \
        .cpu_limit(2) \
        .spec(common_task_spec) \
        .end_task() \
        .begin_task("history") \
        .job_count(1) \
        .command(history_command) \
        .memory_limit(spark_history_server_memory_limit) \
        .cpu_limit(1) \
        .spec(common_task_spec) \
        .end_task() \
        .begin_task("workers") \
        .job_count(spark_worker_count) \
        .command(worker_command) \
        .memory_limit(spark_worker_memory_limit + spark_worker_tmpfs_limit) \
        .cpu_limit(spark_worker_core_count + 2) \
        .spec(common_task_spec) \
        .tmpfs_path("tmpfs") \
        .end_task() \
        .secure_vault(secure_vault) \
        .max_failed_job_count(5) \
        .max_stderr_count(150) \
        .title(operation_alias or "spark_{}".format(user)) \
        .spec(operation_spec)


def _parse_memory(memory):
    if isinstance(memory, int):
        return memory
    units = {"gb": 1024 * 1024 * 1024, "mb": 1024 * 1024, "kb": 1024, "bb": 1, "b": 1}
    if memory is None:
        return None
    m = re.match(r"(\d+)(.*)", memory)
    value = int(m.group(1))
    unit = m.group(2).lower().strip()
    if len(unit) <= 1:
        unit = unit + "b"
    return value * units[unit]


def start_spark_cluster(spark_worker_core_count,
                        spark_worker_memory_limit,
                        spark_worker_count,
                        operation_alias=None,
                        discovery_path=None,
                        pool=None,
                        spark_worker_tmpfs_limit=None,
                        spark_master_memory_limit=None,
                        spark_history_server_memory_limit=None,
                        dynamic_config_path=None,
                        client=None):
    """Start Spark Standalone cluster in YT Vanilla Operation. See https://wiki.yandex-team.ru/spyt/
    :param spark_worker_core_count: Number of cores that will be available on Spark worker
    :param spark_worker_memory_limit: Amount of memory that will be available on Spark worker
    :param spark_worker_count: Number of Spark workers
    :param operation_alias: Alias for the underlying YT operation
    :param discovery_path: Cypress path for discovery files and logs, the same path must be used in find_spark_cluster
    :param pool: Pool for the underlying YT operation
    :param spark_worker_tmpfs_limit: Limit of tmpfs usage per Spark worker
    :param spark_master_memory_limit: Memory limit on Spark master
    :param spark_history_server_memory_limit: Memory limit on Spark History Server
    :param dynamic_config_path: YT path of dynamic config
    :param client: YtClient
    :return: running YT Vanilla Operation with Spark Standalone
    """
    spark_discovery = SparkDiscovery(discovery_path=discovery_path)
    dynamic_config = _read_launch_config(dynamic_config_path=dynamic_config_path, client=client)

    spec_builder = build_spark_operation_spec(
        operation_alias=operation_alias,
        spark_discovery=spark_discovery,
        dynamic_config=dynamic_config,
        spark_worker_core_count=spark_worker_core_count,
        spark_worker_memory_limit=_parse_memory(spark_worker_memory_limit),
        spark_worker_count=spark_worker_count,
        spark_worker_tmpfs_limit=_parse_memory(spark_worker_tmpfs_limit),
        spark_master_memory_limit=_parse_memory(spark_master_memory_limit),
        spark_history_server_memory_limit=_parse_memory(spark_history_server_memory_limit),
        pool=pool,
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
