from library.python.testing.recipe import declare_recipe

import yt.yson as yson

from mapreduce.yt.python.recipe.lib import start, stop


def my_start(args):
    assert not args
    args = [
        "--node-config",
        yson.dumps(
            {
                "bus_server": {
                    "bind_retry_count": 1,
                },
                "tablet_node": {
                    "hydra_manager": {
                        "disable_leader_lease_grace_delay": True,
                        "leader_lease_timeout": 100000,
                    },
                },
            },
            yson_format="text",
        ),
        "--wait-tablet-cell-initialization"
    ]
    return start(args)


def main():
    declare_recipe(my_start, stop)
