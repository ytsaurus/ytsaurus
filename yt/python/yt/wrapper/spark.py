from __future__ import print_function

from .cypress_commands import list as yt_list, create, exists, get
from .http_helpers import get_token, get_user_name, get_proxy_url
from .operation_commands import TimeWatcher, process_operation_unsuccesful_finish_state
from .run_operation_commands import run_operation
from .spec_builders import VanillaSpecBuilder
from .ypath import YPath
from .errors import YtHttpResponseError
from .operation_commands import get_operation_url
from .common import require, update_inplace

import os
import re
import logging


class SparkDefaultArguments(object):
    SPARK_WORKER_TMPFS_LIMIT = "150G"
    SPARK_MASTER_MEMORY_LIMIT = "2G"
    SPARK_HISTORY_SERVER_MEMORY_LIMIT = "8G"
    DYNAMIC_CONFIG_PATH = "//sys/spark/bin/releases/spark-launch-conf"
    SPARK_WORKER_TIMEOUT = "5m"

    @staticmethod
    def get_operation_spec():
        return {"annotations": {"is_spark": True}, "max_failed_job_count": 5, "max_stderr_count": 150}


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
            process_operation_unsuccesful_finish_state(op, op.get_error(state))
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
    return get(dynamic_config_path, client=client)


def build_spark_operation_spec(operation_alias, spark_discovery, dynamic_config,
                               spark_worker_core_count, spark_worker_memory_limit,
                               spark_worker_count, spark_worker_timeout,
                               spark_worker_tmpfs_limit, spark_master_memory_limit, spark_history_server_memory_limit,
                               pool, operation_spec, client=None):
    def launcher_command(component):
        unpack_tar = "tar --warning=no-unknown-keyword -xf spark.tgz"
        run_launcher = "/opt/jdk8/bin/java -Xmx512m -cp spark-yt-launcher.jar"
        spark_conf = []
        for key, value in dynamic_config["spark_conf"].items():
            spark_conf.append("-D{}={}".format(key, value))
        spark_conf = " ".join(spark_conf)

        return "{0} && {1} {2} ru.yandex.spark.launcher.{3}Launcher ".format(unpack_tar, run_launcher,
                                                                             spark_conf, component)

    master_command = launcher_command("Master")
    worker_command = launcher_command("Worker") + \
        "--cores {0} --memory {1} --wait-master-timeout {2}".format(spark_worker_core_count,
                                                                    _format_memory(spark_worker_memory_limit),
                                                                    spark_worker_timeout)
    history_command = launcher_command("HistoryServer") + "--log-path yt:/{}".format(spark_discovery.event_log())

    environment = dynamic_config["environment"]
    environment["YT_PROXY"] = get_proxy_url(required=True, client=client)
    environment["SPARK_DISCOVERY_PATH"] = str(spark_discovery.discovery())

    user = get_user_name(client=client)

    custom_operation_spec = operation_spec or {}
    operation_spec = SparkDefaultArguments.get_operation_spec()
    operation_spec["stderr_table_path"] = str(spark_discovery.stderr())
    operation_spec["pool"] = pool
    update_inplace(operation_spec, custom_operation_spec)

    if "title" not in operation_spec:
        operation_spec["title"] = operation_alias or "spark_{}".format(user)

    common_task_spec = {
        "restart_completed_jobs": True,
        "file_paths": dynamic_config["file_paths"],
        "layer_paths": dynamic_config["layer_paths"],
        "environment": environment,
        "memory_reserve_factor": 1.0
    }

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
        .spec(operation_spec)  # noqa


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
                        spark_worker_timeout=SparkDefaultArguments.SPARK_WORKER_TIMEOUT,
                        operation_alias=None,
                        discovery_path=None,
                        pool=None,
                        spark_worker_tmpfs_limit=SparkDefaultArguments.SPARK_WORKER_TMPFS_LIMIT,
                        spark_master_memory_limit=SparkDefaultArguments.SPARK_MASTER_MEMORY_LIMIT,
                        spark_history_server_memory_limit=SparkDefaultArguments.SPARK_HISTORY_SERVER_MEMORY_LIMIT,
                        dynamic_config_path=SparkDefaultArguments.DYNAMIC_CONFIG_PATH,
                        operation_spec=None,
                        client=None):
    """Start Spark Standalone cluster in YT Vanilla Operation. See https://ytsaurus.tech/docs/en/user-guide/data-processing/spyt/overview
    :param spark_worker_core_count: Number of cores that will be available on Spark worker
    :param spark_worker_memory_limit: Amount of memory that will be available on Spark worker
    :param spark_worker_count: Number of Spark workers
    :param spark_worker_timeout: Worker timeout to wait master start
    :param operation_alias: Alias for the underlying YT operation
    :param discovery_path: Cypress path for discovery files and logs, the same path must be used in find_spark_cluster
    :param pool: Pool for the underlying YT operation
    :param spark_worker_tmpfs_limit: Limit of tmpfs usage per Spark worker
    :param spark_master_memory_limit: Memory limit on Spark master
    :param spark_history_server_memory_limit: Memory limit on Spark History Server
    :param dynamic_config_path: YT path of dynamic config
    :param operation_spec: YT Vanilla Operation spec
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
        spark_worker_timeout=spark_worker_timeout,
        spark_worker_tmpfs_limit=_parse_memory(spark_worker_tmpfs_limit),
        spark_master_memory_limit=_parse_memory(spark_master_memory_limit),
        spark_history_server_memory_limit=_parse_memory(spark_history_server_memory_limit),
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
