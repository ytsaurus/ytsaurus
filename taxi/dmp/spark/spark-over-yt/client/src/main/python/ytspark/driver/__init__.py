import yaml
from pyspark import SparkConf
from pyspark.sql import SparkSession

from ..utils import *


def create_spark_session(spark_id, config_path=None, spark_config=None):
    if config_path is not None:
        with open(config_path) as f:
            config = yaml.load(f)
    else:
        config = {}

    yt_proxy = config.get("yt_proxy", os.getenv("YT_PROXY"))
    yt_user = config.get("yt_user") or os.getenv("YT_USER") or os.getenv("USER")
    yt_token = config.get("yt_token") or os.getenv("YT_TOKEN") or default_token()
    yt_client = create_yt_client(yt_proxy, yt_token)
    discovery_dir = config.get("discovery_dir") or default_discovery_dir(yt_user)
    log_dir = config.get("log_dir") or default_base_log_dir(discovery_dir)
    master = get_spark_master(spark_id, discovery_dir, rest=False, yt_client=yt_client)
    config = SparkConf()
    for (key, value) in base_spark_conf(yt_proxy, yt_user, log_dir).items():
        config.set(key, value)

    if spark_config:
        for (key, value) in spark_config.items():
            config.set(key, value)

    return SparkSession.builder.config(conf=config).master(master).getOrCreate()
