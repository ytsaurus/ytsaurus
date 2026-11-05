from yt.recipe.basic.cluster_factory import yt_cluster_factory

from yt.recipe.chaos.lib import recipe as chaos_yt_recipe

from library.python.testing import recipe

import yatest.common


def start(args):
    work_dir = yatest.common.output_ram_drive_path("yt_wd")
    if work_dir is None:
        work_dir = yatest.common.output_path("yt_wd")

    chaos_yt_recipe.start(yt_cluster_factory, args, work_dir=work_dir)


def stop(args):
    chaos_yt_recipe.stop(args)


if __name__ == "__main__":
    recipe.declare_recipe(start, stop)
