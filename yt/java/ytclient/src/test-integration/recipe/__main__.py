from library.python.testing.recipe import declare_recipe, set_env

from mapreduce.yt.python.yt_stuff import YtStuff, YtConfig

import yatest.common

import yt.wrapper

import os
import json


info_files = []


def start(args, recipe_info_json_file, yt_config):
    if yt_config is None:
        yt_config = YtConfig()

    yt_stuff = YtStuff(yt_config)
    yt_stuff.start_local_yt()

    recipe_info = {
        "yt_id": yt_stuff.yt_id,
        "yt_work_dir": yt_stuff.yt_work_dir,
        "yt_local_exec": yt_stuff.yt_local_exec
    }

    with open(recipe_info_json_file, "w") as fout:
        json.dump(recipe_info, fout)
    return yt_stuff


def stop(args, recipe_info_json_file):
    with open(recipe_info_json_file) as f:
        recipe_info = json.load(f)
    yatest.common.execute(
        recipe_info["yt_local_exec"] + [
            "stop",
            os.path.join(
                recipe_info["yt_work_dir"],
                recipe_info["yt_id"],
            )
        ]
    )


def start_clusters(args, cluster_count):
    info_files = ["yt_recipe_info_{}.json".format(i) for i in range(cluster_count)]
    yts = []
    for i in range(cluster_count):
        yt_config = YtConfig(node_count=3, cell_tag=i)
        yts.append(start(args, yt_config=yt_config, recipe_info_json_file=info_files[i]))
    return yts


def stop_clusters(args):
    for info_file in info_files:
        stop(args, recipe_info_json_file=info_file)


def _get_client(yt_stuff):
    client = yt.wrapper.YtClient()
    client.config["proxy"]["url"] = "localhost:" + str(yt_stuff.yt_proxy_port)
    client.config["proxy"]["enable_proxy_discovery"] = False
    return client


def start_for_remote_copy(args):
    clusters = start_clusters(args, 3)
    set_env("YT_PROXY_ONE", "localhost:" + str(clusters[0].yt_proxy_port))
    set_env("YT_PROXY_TWO", "localhost:" + str(clusters[1].yt_proxy_port))
    set_env("YT_PROXY_THREE", "localhost:" + str(clusters[2].yt_proxy_port))

    return clusters


if __name__ == "__main__":
    declare_recipe(start_for_remote_copy, stop_clusters)
