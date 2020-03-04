import sys

sys.path.append("/usr/lib/yandex/spark/python")

import yaml
import re
from pyspark import SparkConf
from pyspark.sql import SparkSession

from yt_spark_client.utils import default_token, default_discovery_dir, get_spark_master, set_conf, SparkDiscovery
from contextlib import contextmanager
import os
from yt.wrapper import YtClient
from yt.wrapper.http_helpers import get_token, get_user_name, get_proxy_url


def _parse_memory(memory_str):
    units = {"gb": 1024 * 1024 * 1024, "mb": 1024 * 1024, "kb": 1024, "bb": 1, "b": 1}
    if memory_str is None:
        return None
    m = re.match("(\d+)(.*)", memory_str)
    value = int(m.group(1))
    unit = m.group(2).lower().strip()
    if len(unit) <= 1:
        unit = unit + "b"
    return value * units[unit]


def _format_memory(memory_bytes):
    units = {"gb": 1024 * 1024 * 1024, "mb": 1024 * 1024, "kb": 1024, "bb": 1, "b": 1}
    if memory_bytes % units["gb"] == 0:
        return "{}G".format(memory_bytes // units["gb"])
    if memory_bytes % units["mb"] == 0:
        return "{}M".format(memory_bytes // units["mb"])
    if memory_bytes % units["kb"] == 0:
        return "{}K".format(memory_bytes // units["kb"])
    return "{}B".format(memory_bytes)


@contextmanager
def spark_session(num_executors=None,
                  yt_proxy=None,
                  discovery_path=None,
                  config_path=None,
                  app_name=None,
                  cores_per_executor=None,
                  executor_memory_per_core=None,
                  driver_memory=None,
                  dynamic_allocation=False,
                  spark_conf_args=None,
                  spark_id=None,
                  client=None):
    conf = _build_spark_config(
        num_executors=num_executors,
        yt_proxy=yt_proxy,
        discovery_path=discovery_path,
        config_path=config_path,
        app_name=app_name,
        cores_per_executor=cores_per_executor,
        executor_memory_per_core=executor_memory_per_core,
        driver_memory=driver_memory,
        dynamic_allocation=dynamic_allocation,
        spark_conf_args=spark_conf_args,
        spark_id=spark_id,
        client=client
    )
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()
        spark._jvm.ru.yandex.spark.yt.fs.YtClientProvider.close()


def get_master_from_discovery(discovery_path, spark_id, config, client):
    spark_id = spark_id or config.get("spark_id")
    discovery_path = discovery_path or config.get("discovery_path") or config.get(
        "discovery_dir") or default_discovery_dir()
    discovery = SparkDiscovery(discovery_path=discovery_path, spark_id=spark_id)
    master = get_spark_master(discovery, rest=False, yt_client=client)
    return master


def get_log_dir_from_discovery(discovery_path, spark_id, config):
    spark_id = spark_id or config.get("spark_id")
    discovery_path = discovery_path or config.get("discovery_path") or config.get(
        "discovery_dir") or default_discovery_dir()
    discovery = SparkDiscovery(discovery_path=discovery_path, spark_id=spark_id)
    return discovery.event_log()


def create_yt_client(yt_proxy, config):
    yt_proxy = yt_proxy or config.get("yt_proxy", os.getenv("YT_PROXY"))
    yt_token = config.get("yt_token") or default_token()
    return YtClient(proxy=yt_proxy, token=yt_token)


def _read_config(config_path):
    config_path = config_path or os.path.join(os.getenv("HOME"), "spyt.yaml")
    if os.path.isfile(config_path):
        with open(config_path) as f:
            return yaml.load(f, Loader=yaml.BaseLoader)
    return {}


def _configure_client_mode(spark_conf,
                           discovery_path,
                           local_conf,
                           spark_id,
                           client=None):
    master = get_master_from_discovery(discovery_path, spark_id, local_conf, client)
    event_log_dir = get_log_dir_from_discovery(discovery_path, spark_id, local_conf)
    yt_proxy = get_proxy_url(required=True, client=client)
    yt_user = get_user_name(client=client)
    spark_conf.set("spark.master", master)
    spark_conf.set("spark.eventLog.dir", event_log_dir)
    spark_conf.set("spark.hadoop.yt.proxy", yt_proxy)
    spark_conf.set("spark.hadoop.yt.user", yt_user)
    spark_conf.set("spark.hadoop.yt.token", get_token(client=client))


def _validate_resources(num_executors,
                        cores_per_executor,
                        executor_memory_per_core,
                        driver_memory,
                        executor_memory):
    _MAX_CORES = 32
    _MAX_MEMORY = _parse_memory("64G")
    _MIN_MEMORY = _parse_memory("512Mb")
    _MAX_EXECUTORS = 100
    _MAX_TOTAL_CORES = 400
    _MAX_TOTAL_MEMORY = _parse_memory("1024G")

    if driver_memory and (driver_memory < _MIN_MEMORY or driver_memory > _MAX_MEMORY):
        raise AssertionError("Invalid amount of driver memory")

    if num_executors and (not isinstance(num_executors, int) or num_executors < 1 or num_executors > _MAX_EXECUTORS):
        raise AssertionError("Invalid number of executors")

    if cores_per_executor and (not isinstance(cores_per_executor,
                                              int) or cores_per_executor < 1 or cores_per_executor > _MAX_CORES):
        raise AssertionError("Invalid number of cores per executor")

    if executor_memory_per_core and executor_memory_per_core < 1:
        raise AssertionError("Invalid amount of memory per core")

    if executor_memory and (executor_memory > _MAX_MEMORY or executor_memory < _MIN_MEMORY):
        raise AssertionError("Invalid amount of memory per executor")

    if executor_memory and num_executors and executor_memory * num_executors > _MAX_TOTAL_MEMORY:
        raise AssertionError("Invalid amount of total memory")

    if cores_per_executor and num_executors and cores_per_executor * num_executors > _MAX_TOTAL_CORES:
        raise AssertionError("Invalid amount of total cores")


def _set_none_safe(conf, key, value):
    if value:
        conf.set(key, value)


def _build_spark_config(num_executors=None,
                        yt_proxy=None,
                        discovery_path=None,
                        config_path=None,
                        app_name=None,
                        cores_per_executor=None,
                        executor_memory_per_core=None,
                        driver_memory=None,
                        dynamic_allocation=None,
                        spark_conf_args=None,
                        spark_id=None,
                        client=None):
    is_client_mode = os.getenv("USER") is not None

    local_conf = _read_config(config_path) if is_client_mode else {}
    if is_client_mode:
        app_name = app_name or "PySpark for {}".format(os.getenv("USER"))

    spark_conf = SparkConf()

    if is_client_mode:
        client = client or create_yt_client(yt_proxy, local_conf)
        _configure_client_mode(spark_conf, discovery_path, local_conf, spark_id, client)

    num_executors = num_executors or local_conf.get("num_executors")
    cores_per_executor = cores_per_executor or local_conf.get("cores_per_executor")
    executor_memory_per_core = _parse_memory(executor_memory_per_core or local_conf.get("executor_memory_per_core"))
    driver_memory = _parse_memory(driver_memory or local_conf.get("driver_memory"))
    executor_memory = executor_memory_per_core * cores_per_executor if executor_memory_per_core and cores_per_executor else None
    _validate_resources(num_executors, cores_per_executor, executor_memory_per_core, driver_memory, executor_memory)
    max_cores = num_executors * cores_per_executor if num_executors and cores_per_executor else None

    _set_none_safe(spark_conf, "spark.dynamicAllocation.enabled", dynamic_allocation)
    _set_none_safe(spark_conf, "spark.dynamicAllocation.executorIdleTimeout", "10m")
    _set_none_safe(spark_conf, "spark.cores.max", max_cores)
    _set_none_safe(spark_conf, "spark.dynamicAllocation.maxExecutors", num_executors)
    _set_none_safe(spark_conf, "spark.executor.cores", cores_per_executor)
    _set_none_safe(spark_conf, "spark.executor.memory", _format_memory(executor_memory) if executor_memory else None)
    _set_none_safe(spark_conf, "spark.driver.memory", _format_memory(driver_memory) if driver_memory else None)
    _set_none_safe(spark_conf, "spark.driver.maxResultSize", _format_memory(driver_memory) if driver_memory else None)
    _set_none_safe(spark_conf, "spark.app.name", app_name)
    set_conf(spark_conf, spark_conf_args)

    return spark_conf


def connect(num_executors=5,
            yt_proxy=None,
            discovery_path=None,
            config_path=None,
            app_name=None,
            cores_per_executor=4,
            executor_memory_per_core="4G",
            driver_memory="1G",
            dynamic_allocation=False,
            spark_conf_args=None,
            spark_id=None):
    conf = _build_spark_config(
        num_executors=num_executors,
        yt_proxy=yt_proxy,
        discovery_path=discovery_path,
        config_path=config_path,
        app_name=app_name,
        cores_per_executor=cores_per_executor,
        executor_memory_per_core=executor_memory_per_core,
        driver_memory=driver_memory,
        dynamic_allocation=dynamic_allocation,
        spark_conf_args=spark_conf_args,
        spark_id=spark_id,
        client=create_yt_client(yt_proxy, _read_config(config_path))
    )

    return SparkSession.builder.config(conf=conf).getOrCreate()
