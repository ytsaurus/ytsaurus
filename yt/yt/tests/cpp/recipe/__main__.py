from __future__ import print_function

from yt.environment import arcadia_interop

import yt.local

import yt.yson as yson

import yatest.common

from library.python.testing.recipe import declare_recipe, set_env

import argparse
import glob
import os
import shutil
import sys


RECIPE_INFO_FILE = "recipe_info.yson"


def get_output_dir():
    output_ram_drive_path = arcadia_interop.yatest_common.output_ram_drive_path()
    if output_ram_drive_path is None:
        return arcadia_interop.yatest_common.output_path()
    else:
        return output_ram_drive_path


def prepare_yatest_environment():
    destination = os.path.join(get_output_dir(), "build")
    os.makedirs(destination)
    path = arcadia_interop.prepare_yt_environment(destination, copy_ytserver_all="YT_OUTPUT" not in os.environ, need_suid=False)
    os.environ["PATH"] = os.pathsep.join([path, os.environ.get("PATH", "")])


# Why??????
OPTION_NAME_MAPPING = {
    "NUM_MASTERS": "master_count",
    "NUM_NODES": "node_count",
    "NUM_SCHEDULERS": "scheduler_count",
    "NUM_CONTROLLER_AGENTS": "controller_agent_count",
    "DELTA_MASTER_CONFIG": "master_config",
    "RPC_PROXY_COUNT": "rpc_proxy_count",
    "RPC_PROXY_CONFIG": "rpc_proxy_config"
}


def start(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster-config", help="YSON file containing options for yt_local")
    parsed_args = parser.parse_args(args)

    prepare_yatest_environment()
    path = os.path.join(get_output_dir(), "yt_wd")

    if parsed_args.cluster_config is not None:
        options_file_path = yatest.common.test_source_path(parsed_args.cluster_config)
        with open(options_file_path, "rb") as f:
            options = yson.load(f)
    else:
        options = {}

    driver_backend = "native"
    if "DRIVER_BACKEND" in options:
        driver_backend = options["DRIVER_BACKEND"]
        del options["DRIVER_BACKEND"]

    yt_local_args = {
        "path": path,
        "enable_debug_logging": True,
    }
    for k, v in options.iteritems():
        yt_local_args[OPTION_NAME_MAPPING[k]] = v
    yt_instance = yt.local.start(**yt_local_args)

    recipe_info = {
        "yt_local_id": yt_instance.id,
        "yt_local_path": path,
    }

    with open(RECIPE_INFO_FILE, "w") as fout:
        yson.dump(recipe_info, fout)

    if driver_backend == "native":
        set_env("YT_DRIVER_CONFIG_PATH", yt_instance.config_paths["driver"])
        set_env("YT_DRIVER_LOGGING_CONFIG_PATH", yt_instance.config_paths["driver_logging"])
    elif driver_backend == "rpc":
        set_env("YT_NATIVE_DRIVER_CONFIG_PATH", yt_instance.config_paths["driver"])
        set_env("YT_DRIVER_CONFIG_PATH", yt_instance.config_paths["rpc_driver"])
        set_env("YT_DRIVER_LOGGING_CONFIG_PATH", yt_instance.config_paths["driver_logging"])
    else:
        print("Incorrect driver backend '%s'", driver_backend, file=sys.stderr)
        sys.exit(1)


def clear_runtime_data(path):
    runtime_data = [os.path.join(path, "runtime_data")] + glob.glob(path + "/*/runtime_data")
    for dir in runtime_data:
        if os.path.exists(dir):
            shutil.rmtree(dir, ignore_errors=True)


def stop(args):
    if not os.path.exists(RECIPE_INFO_FILE):
        return
    with open(RECIPE_INFO_FILE) as fin:
        recipe_info = yson.load(fin)
    yt.local.stop(recipe_info["yt_local_id"], path=recipe_info["yt_local_path"])
    clear_runtime_data(os.path.join(recipe_info["yt_local_path"], recipe_info["yt_local_id"]))


if __name__ == "__main__":
    declare_recipe(start, stop)
