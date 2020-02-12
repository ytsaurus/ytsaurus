import argparse

import os
import re

import yt
from yt.wrapper.cypress_commands import list as yt_list
from yt.wrapper import YPath


def parse_memory(memory_str):
    units = {"gb": 1024 * 1024 * 1024, "mb": 1024 * 1024, "kb": 1024, "bb": 1, "b": 1}
    if memory_str is None:
        return None
    m = re.match(r"(\d+)(.*)", memory_str)
    value = int(m.group(1))
    unit = m.group(2).lower().strip()
    if len(unit) <= 1:
        unit = unit + "b"
    return value * units[unit]


def format_memory(memory_bytes):
    units = {"gb": 1024 * 1024 * 1024, "mb": 1024 * 1024, "kb": 1024, "bb": 1, "b": 1}
    if memory_bytes % units["gb"] == 0:
        return "{}G".format(memory_bytes / units["gb"])
    if memory_bytes % units["mb"] == 0:
        return "{}M".format(memory_bytes / units["mb"])
    if memory_bytes % units["kb"] == 0:
        return "{}K".format(memory_bytes / units["kb"])
    return "{}B".format(memory_bytes)


def get_spark_master(spark_id, discovery_dir, rest, yt_client):
    master_path = "rest" if rest else "address"
    master = yt_list(YPath(discovery_dir).join("instances").join(spark_id).join(master_path), client=yt_client)[0]
    return "spark://{0}".format(master)


def create_yt_client(yt_proxy, yt_token):
    return yt.wrapper.YtClient(proxy=yt_proxy, token=yt_token)


def default_token():
    with open(os.path.join(os.getenv("HOME"), ".yt", "token")) as f:
        token = f.readline().strip()
    return token


def base_spark_conf(yt_proxy, yt_user, log_dir):
    return {
        "spark.hadoop.yt.proxy": yt_proxy,
        "spark.hadoop.yt.user": yt_user,
        "spark.master.rest.enabled": "true",
        "spark.eventLog.dir": "yt:/{}".format(log_dir),
    }


def default_spark_conf(is_dynamic):
    return {
        "spark.dynamicAllocation.enabled": is_dynamic,
        "spark.dynamicAllocation.executorIdleTimeout": "10m",
        "spark.dynamicAllocation.maxExecutors": 5,
        "spark.cores.max": "20",
        "spark.driver.maxResultSize": "1G",
        "spark.driver.memory": "1G",
        "spark.executor.memory": "8G"
    }


def set_conf(conf, dict_conf):
    if dict_conf is not None:
        for (key, value) in dict_conf.items():
            conf.set(key, value)


def default_discovery_dir(yt_user):
    return os.getenv("SPARK_YT_DISCOVERY_DIR") or YPath("//home").join(yt_user).join("spark-tmp")


def default_base_log_dir(discovery_dir):
    return os.getenv("SPARK_YT_LOG_DIR") or YPath(discovery_dir).join("logs")


def default_user():
    return os.getenv("YT_USER") or os.getenv("USER")


def default_proxy():
    return os.getenv("YT_PROXY")


def get_default_arg_parser(**kwargs):
    parser = argparse.ArgumentParser(**kwargs)
    parser.add_argument("--id", required=True)
    parser.add_argument("--discovery-dir", required=False)
    parser.add_argument("--log-dir", required=False)
    parser.add_argument("--proxy", required=False, default=default_proxy())
    parser.add_argument("--yt-user", required=False, default=default_user())
    return parser


def parse_args(parser=None, parser_arguments=None):
    parser_arguments = parser_arguments or {}
    parser = parser or get_default_arg_parser(**parser_arguments)
    args, unknown_args = parser.parse_known_args()
    args.discovery_dir = args.discovery_dir or default_discovery_dir(args.yt_user)
    args.log_dir = args.log_dir or default_base_log_dir(args.discovery_dir).join(args.id)
    return args, unknown_args
