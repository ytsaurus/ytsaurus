import json

from library.python.testing.recipe import declare_recipe, set_env

from mapreduce.yt.python.yt_stuff import YtStuff, YtConfig

import yatest.common

import yt.wrapper

info_files = []


def start(args, recipe_info_json_file, yt_config):
    yt_config.python_binary = yatest.common.binary_path("contrib/tools/python/python")

    yt = YtStuff(yt_config)
    yt.start_local_yt()

    recipe_info = {
        "yt_id": yt.yt_id,
        "yt_work_dir": yt.yt_work_dir,
        "yt_local_path": yt.yt_local_path
    }

    with open(recipe_info_json_file, "w") as f:
        json.dump(recipe_info, f)
    return yt


def stop(args, recipe_info_json_file):
    with open(recipe_info_json_file) as f:
        recipe_info = json.load(f)
    yatest.common.execute(
        recipe_info["yt_local_path"] + [
            "stop",
            os.path.join(
                recipe_info["yt_work_dir"],
                recipe_info["yt_id"],
            )
        ]
    )


def start_clusters(args, cluster_count):
    info_files = ["yt_recipe_info_{}.json".format(i) for i in xrange(cluster_count)]
    yts = []
    for i in xrange(cluster_count):
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
    src, dst = start_clusters(args, 2)
    set_env("YT_PROXY_SRC", "localhost:" + str(src.yt_proxy_port))
    set_env("YT_PROXY_DST", "localhost:" + str(dst.yt_proxy_port))
    src_name = "src"
    set_env("YT_SRC_CLUSTER_NAME", src_name)
    src_client, dst_client = map(_get_client, [src, dst])

    src_primary_master = src_client.list("//sys/primary_masters")[0]
    src_primary_master_config = src_client.get(
        "//sys/primary_masters/{}/orchid/config/primary_master".format(src_primary_master))
    dst_client.set("//sys/clusters/{}".format(src_name), {"primary_master": src_primary_master_config})
    return [src, dst]


if __name__ == "__main__":
    declare_recipe(start_for_remote_copy, stop_clusters)
