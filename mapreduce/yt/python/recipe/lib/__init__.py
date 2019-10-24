from mapreduce.yt.python.yt_stuff import YtStuff, YtConfig

from library.python.testing.recipe import set_env

import yatest.common

import argparse
import os
import json

recipe_info_json_file = "yt_recipe_info.json"


def start(argv, yt_config=None):
    args = parse_args(argv)

    if yt_config is None:
        yt_config = YtConfig()

    if args.local_cypress_source_dir is not None:
        yt_config.local_cypress_dir = yatest.common.test_source_path(args.local_cypress_source_dir)
    if args.local_cypress_work_dir is not None:
        yt_config.local_cypress_dir = yatest.common.work_path(args.local_cypress_work_dir)

    yt_stuff = YtStuff(yt_config)
    yt_stuff.start_local_yt()

    recipe_info = {
        "yt_id": yt_stuff.yt_id,
        "yt_work_dir": yt_stuff.yt_work_dir,
        "yt_local_exec": yt_stuff.yt_local_exec,
    }

    with open(recipe_info_json_file, "w") as fout:
        json.dump(recipe_info, fout)

    os.symlink(
        os.path.join(yt_stuff.yt_work_dir, yt_stuff.yt_id, "info.yson"),
        "info.yson"
    )

    with open("yt_proxy_port.txt", "w") as fout:
        fout.write(str(yt_stuff.yt_proxy_port))

    set_env("YT_PROXY", "localhost:" + str(yt_stuff.yt_proxy_port))

    return yt_stuff


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--local-cypress-work-dir", dest="local_cypress_work_dir",
        default=None, help="Set local cypress dir at test work path"
    )
    parser.add_argument(
        "--local-cypress-source-dir", dest="local_cypress_source_dir",
        default=None, help="Set local cypress dir at test source path"
    )
    return parser.parse_known_args(argv)[0]


def stop(args):
    if not os.path.exists(recipe_info_json_file):
        return

    with open(recipe_info_json_file) as fin:
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
