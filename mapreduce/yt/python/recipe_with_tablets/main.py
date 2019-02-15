from library.python.testing.recipe import declare_recipe

from mapreduce.yt.python.yt_stuff import YtConfig
from mapreduce.yt.python.recipe.lib import start as generic_start, stop


def start(args):
    yt_config = YtConfig(
        wait_tablet_cell_initialization=True,
        node_config={
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
    )
    return generic_start(args, yt_config)


def main():
    declare_recipe(start, stop)
