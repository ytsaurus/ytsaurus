from mapreduce.yt.python.yt_stuff import YtStuff, YtConfig

from library.python.testing.recipe import set_env

import yatest.common

import os
import json

recipe_info_json_file = "yt_recipe_info.json"


def start(args, yt_config=None):
    if yt_config is None:
        yt_config = YtConfig()

    if args:
        yt_config.local_cypress_dir = yatest.common.work_path(args[0])

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
