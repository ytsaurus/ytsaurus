import sys

sys.path.append("/usr/lib/yandex/spark/python")

import yaml
import re
from pyspark import SparkConf
from pyspark.sql import SparkSession

from yt_spark_client.utils import default_token, default_discovery_dir, get_spark_master, set_conf, base_spark_conf, \
    default_spark_conf, SparkDiscovery
from contextlib import contextmanager
import os
from yt.wrapper import YtClient


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
def spark_session(conf=SparkConf()):
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()
        spark._jvm.ru.yandex.spark.yt.fs.YtClientProvider.close()


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
    _MAX_CORES = 32
    _MAX_MEMORY = _parse_memory("64G")
    _MIN_MEMORY = _parse_memory("512Mb")
    _MAX_EXECUTORS = 100
    _MAX_TOTAL_CORES = 400
    _MAX_TOTAL_MEMORY = _parse_memory("1024G")

    config_path = config_path or os.path.join(os.getenv("HOME"), "spyt.yaml")
    if os.path.isfile(config_path):
        with open(config_path) as f:
            config = yaml.load(f, Loader=yaml.BaseLoader)
    else:
        config = {}

    spark_id = spark_id or config.get("spark_id")
    yt_proxy = yt_proxy or config.get("yt_proxy", os.getenv("YT_PROXY"))
    yt_token = config.get("yt_token") or default_token()
    yt_client = YtClient(proxy=yt_proxy, token=yt_token)
    discovery_path = discovery_path or config.get("discovery_path") or config.get("discovery_dir") or default_discovery_dir()

    discovery = SparkDiscovery(discovery_path=discovery_path, spark_id=spark_id)
    master = get_spark_master(discovery, rest=False, yt_client=yt_client)

    num_executors = num_executors or config.get("num_executors")
    cores_per_executor = cores_per_executor or config.get("cores_per_executor")
    executor_memory_per_core = _parse_memory(executor_memory_per_core or config.get("executor_memory_per_core"))
    driver_memory = _parse_memory(driver_memory or config.get("driver_memory"))

    if driver_memory < _MIN_MEMORY or driver_memory > _MAX_MEMORY:
        raise AssertionError("Invalid amount of driver memory")

    if not isinstance(num_executors, int) or num_executors < 1 or num_executors > _MAX_EXECUTORS:
        raise AssertionError("Invalid number of executors")

    if not isinstance(cores_per_executor, int) or cores_per_executor < 1 or cores_per_executor > _MAX_CORES:
        raise AssertionError("Invalid number of cores per executor")

    if executor_memory_per_core < 1:
        raise AssertionError("Invalid amount of memory per core")

    executor_memory = executor_memory_per_core * cores_per_executor

    if executor_memory > _MAX_MEMORY or executor_memory < _MIN_MEMORY:
        raise AssertionError("Invalid amount of memory per executor")

    if executor_memory * num_executors > _MAX_TOTAL_MEMORY:
        raise AssertionError("Invalid amount of total memory")

    if cores_per_executor * num_executors > _MAX_TOTAL_CORES:
        raise AssertionError("Invalid amount of total cores")

    app_name = app_name or "PySpark for {}".format(os.environ['USER'])

    conf = SparkConf()
    set_conf(conf, base_spark_conf(yt_client, discovery))
    set_conf(conf, default_spark_conf(dynamic_allocation))
    set_conf(conf, spark_conf_args)
    conf.set("spark.hadoop.yt.token", yt_token)
    conf.set("spark.cores.max", str(num_executors * cores_per_executor))
    conf.set("spark.dynamicAllocation.maxExecutors", str(num_executors))
    conf.set("spark.executor.cores", str(cores_per_executor))
    conf.set("spark.executor.memory", _format_memory(executor_memory))
    conf.set("spark.driver.memory", _format_memory(driver_memory))

    return SparkSession.builder.config(conf=conf).appName(app_name).master(master).getOrCreate()
