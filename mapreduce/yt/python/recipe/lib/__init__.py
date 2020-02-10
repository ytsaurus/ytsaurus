from mapreduce.yt.python.yt_stuff import YtStuff, YtConfig

import yt.yson as yson

from yt.common import update, get_value
import yt.environment.init_operation_archive

from library.python.testing.recipe import set_env

import yatest.common

import argparse
import os
import json

RECIPE_INFO_FILE = "yt_recipe_info.json"


class ParseStructuredArgument(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        # Multiple times specified arguments are merged into single dict.
        old_value = get_value(getattr(namespace, self.dest), {})
        new_value = update(old_value, yson._loads_from_native_str(values))
        setattr(namespace, self.dest, new_value)

def start(args):
    parser = argparse.ArgumentParser()
    local_cypress_dir_parser = parser.add_mutually_exclusive_group()
    local_cypress_dir_parser.add_argument("--local-cypress-dir-in-source-path")
    local_cypress_dir_parser.add_argument("--local-cypress-dir-in-work-path")
    parser.add_argument("--wait-tablet-cell-initialization", action="store_true")
    parser.add_argument("--node-config", action=ParseStructuredArgument)
    parser.add_argument("--master-config", action=ParseStructuredArgument)
    parser.add_argument("--proxy-config", action=ParseStructuredArgument)
    parser.add_argument("--scheduler-config", action=ParseStructuredArgument)
    parser.add_argument("--controller-agent-config", action=ParseStructuredArgument)
    parser.add_argument("--job-controller-resource-limits", action=ParseStructuredArgument)
    parser.add_argument("--node-chunk-store-quota", type=int)
    parser.add_argument("--init-operation-archive", action="store_true")
    parsed_args, _ = parser.parse_known_args(args)

    config_args = dict((key, value) for key, value in vars(parsed_args).items() if value is not None)

    init_operation_archive = config_args.get("init_operation_archive", False)
    del config_args["init_operation_archive"]

    if "local_cypress_dir_in_source_path" in config_args:
        config_args["local_cypress_dir"] = yatest.common.test_source_path(config_args["local_cypress_dir_in_source_path"])
        del config_args["local_cypress_dir_in_source_path"]
    if "local_cypress_dir_in_work_path" in config_args:
        config_args["local_cypress_dir"] = yatest.common.work_path(config_args["local_cypress_dir_in_work_path"])
        del config_args["local_cypress_dir_in_work_path"]

    yt_config = YtConfig(**config_args)
    yt_stuff = YtStuff(yt_config)
    yt_stuff.start_local_yt()

    recipe_info = {
        "yt_id": yt_stuff.yt_id,
        "yt_work_dir": yt_stuff.yt_work_dir,
        "yt_local_exec": yt_stuff.yt_local_exec,
    }

    with open(RECIPE_INFO_FILE, "w") as fout:
        json.dump(recipe_info, fout)

    os.symlink(
        os.path.join(yt_stuff.yt_work_dir, yt_stuff.yt_id, "info.yson"),
        "info.yson"
    )

    with open("yt_proxy_port.txt", "w") as fout:
        fout.write(str(yt_stuff.yt_proxy_port))

    set_env("YT_PROXY", "localhost:" + str(yt_stuff.yt_proxy_port))

    if init_operation_archive:
        yt.environment.init_operation_archive.create_tables_latest_version(yt_stuff.get_yt_client())

    return yt_stuff


def stop(args):
    if not os.path.exists(RECIPE_INFO_FILE):
        return

    with open(RECIPE_INFO_FILE) as fin:
        recipe_info = json.load(fin)

    yatest.common.execute(
        recipe_info["yt_local_exec"] + [
            "stop",
            os.path.join(
                recipe_info["yt_work_dir"],
                recipe_info["yt_id"]
            )
        ]
    )
