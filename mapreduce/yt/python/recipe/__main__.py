import os
import json

from library.python.testing.recipe import declare_recipe, set_env

import yatest.common

from mapreduce.yt.python.yt_stuff import YtStuff, YtConfig

recipe_info_json_file = "yt_recipe_info.json"


def start(args):
    yt_config = YtConfig(python_binary=yatest.common.binary_path("contrib/tools/python/python"))
    yt = YtStuff(yt_config)
    yt.start_local_yt()

    recipe_info = {
        "yt_id": yt.yt_id,
        "yt_work_dir": yt.yt_work_dir,
        "yt_local_path": yt.yt_local_path
    }

    with open(recipe_info_json_file, "w") as f:
        json.dump(recipe_info, f)

    os.symlink(
        os.path.join(yt.yt_work_dir, yt.yt_id, "info.yson"),
        "info.yson"
    )

    with open("yt_proxy_port.txt", "w") as f:
        f.write(str(yt.yt_proxy_port))

    set_env("YT_PROXY", "localhost:" + str(yt.yt_proxy_port))


def stop(args):
    with open(recipe_info_json_file) as f:
        recipe_info = json.load(f)

    yatest.common.execute(
        recipe_info["yt_local_path"] + [
            "stop",
            os.path.join(
                recipe_info["yt_work_dir"],
                recipe_info["yt_id"]
            )
        ]
    )

if __name__ == "__main__":
    declare_recipe(start, stop)
