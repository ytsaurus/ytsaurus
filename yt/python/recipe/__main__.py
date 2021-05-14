import argparse
import json
import os

import yatest.common
from yt.python.recipe.lib import dump_yt_cluster, local_yt
from library.python.testing import recipe

RECIPE_INFO_FILE = "yt_recipe_info.json"


def _start_local_yt(config, acl_groups):
    """start local yt replicas according to config."""
    clusters = local_yt.start(replicas=config, groups=acl_groups)

    recipe_info = [
        {"yt_id": x.yt_id, "yt_work_dir": x.yt_work_dir, "yt_local_exec": x.yt_local_exec}
        for x in clusters.values()
    ]

    with open(os.path.join(recipe.ya.output_dir, RECIPE_INFO_FILE), "w") as fd:
        json.dump(recipe_info, fd)
    return clusters


def _dump_servers_list(servers):
    """save proxy list to env variables and local file."""
    for server in servers:
        recipe.set_env("YT_PROXY_%s" % server.yt_id.upper(), "localhost:%d" % server.yt_proxy_port)

    dump_yt_cluster(servers)


def split(s):
    return [x.strip() for x in s.split(",")] if s else []


def start(args):
    """recipe entry point (start services)."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-R", "--replica", type=split, required=True, help="yt replicas")
    parser.add_argument(
        "--replica-config", default={}, type=json.loads, help="json config for replica"
    )
    parser.add_argument("-G", "--acl-group", type=split, required=False, default=[], help="YT acl groups, if needed")
    parsed_args, _ = parser.parse_known_args(args)

    servers = _start_local_yt(
        config={x: parsed_args.replica_config for x in parsed_args.replica},
        acl_groups=parsed_args.acl_group
    )
    servers = [servers[x] for x in parsed_args.replica]
    _dump_servers_list(servers)
    # set yt specific variables
    recipe.set_env("YT_PROXY", "localhost:%d" % servers[0].yt_proxy_port)
    recipe.set_env("YT_USER", "root")
    recipe.set_env("YT_TOKEN", "yt_token")


def stop(_):
    """recipe entry point (stop services)."""
    recipe_file = os.path.join(recipe.ya.output_dir, RECIPE_INFO_FILE)
    if not os.path.exists(recipe_file):
        return

    with open(recipe_file, "r") as fd:
        recipe_info = json.load(fd)

    for info in recipe_info:
        yatest.common.execute(
            info["yt_local_exec"] + ["stop", os.path.join(info["yt_work_dir"], info["yt_id"])]
        )


if __name__ == "__main__":
    recipe.declare_recipe(start, stop)
