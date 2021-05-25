from mapreduce.yt.python.recipe_multi_cluster.lib import yt_cluster_factory
from yt.recipe.multi_cluster.lib import recipe as yt_recipe

from library.python.testing import recipe


def start(args):
    yt_recipe.start(yt_cluster_factory, args)


def stop(args):
    yt_recipe.stop(args)


if __name__ == "__main__":
    recipe.declare_recipe(start, stop)
