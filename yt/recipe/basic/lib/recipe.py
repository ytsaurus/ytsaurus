from . import dump_yt_clusters, dump_yt_instances, get_yt_instances, local_yt, run_concurrent

from library.python.testing import recipe

from yt.wrapper import cli_helpers

from deepmerge import always_merger

import argparse
import copy
import shutil
import os


def _merge(base, patch):
    return always_merger.merge(copy.deepcopy(base), patch)


def _start_local_yt(
    yt_cluster_factory,
    cluster_names,
    package_dir,
    config_patches=None,
    cluster_config_patches=None,
    work_dir=None,
):
    """start local yt replicas according to config."""
    clusters = local_yt.start(
        yt_cluster_factory,
        cluster_names,
        config_patches=config_patches,
        cluster_config_patches=cluster_config_patches,
        work_dir=work_dir,
        package_dir=package_dir,
    )

    dump_yt_instances(clusters)

    return clusters


def _dump_clusters_list(clusters):
    """save proxy list to env variables and local file."""
    for cluster in clusters:
        recipe.set_env("YT_PROXY_%s" % cluster.yt_id.upper(), cluster.get_proxy_address())

    dump_yt_clusters(clusters)


def _split(s):
    return [x.strip() for x in s.split(",")] if s else []


def start(yt_cluster_factory, args, work_dir=None):
    """recipe entry point (start services)."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster-names", type=_split, required=True, help="list of YT cluster names")

    parser.add_argument("--cluster-config-patches", action=cli_helpers.ParseStructuredArgument, default=None,
                        help="config patch for YT cluster in yson format {id={name=value;...};}")
    parser.add_argument("--config-patch", action=cli_helpers.ParseStructuredArguments,
                        help="the patches in yson format {name=value;...} which will be applied to all of clusters", dest="config_patches", required=False)

    parser.add_argument("--cleanup-working-directory", action="store_true", default=False,
                        help="clean working directory before recipe start")
    parser.add_argument("--package-dir", help="where to take YT binaries from", default="yt/packages/latest")

    parsed_args, _ = parser.parse_known_args(args)

    if work_dir and parsed_args.cleanup_working_directory and os.path.isdir(work_dir):
        shutil.rmtree(work_dir)

    clusters = _start_local_yt(
        yt_cluster_factory,
        cluster_names=parsed_args.cluster_names,
        config_patches=parsed_args.config_patches,
        cluster_config_patches=parsed_args.cluster_config_patches,
        work_dir=work_dir,
        package_dir=parsed_args.package_dir,
    )
    clusters_list = [clusters[x] for x in parsed_args.cluster_names]
    _dump_clusters_list(clusters_list)

    # Set YT specific variables.
    recipe.set_env("YT_PROXY", clusters_list[0].get_proxy_address())
    recipe.set_env("YT_USER", "root")  # TODO(nadya73): check if it is needed, remove it?
    recipe.set_env("YT_TOKEN", "yt_token")  # TODO(nadya73): check if it is needed, remove it?
    if "YT_PROXY_URL_ALIASING_CONFIG" in os.environ:  # TODO(nadya73): to check what is it?
        recipe.set_env("YT_PROXY_URL_ALIASING_CONFIG", os.environ["YT_PROXY_URL_ALIASING_CONFIG"])

    return clusters


def stop(_):
    """recipe entry point (stop services)."""
    instances = get_yt_instances()
    run_concurrent(lambda idx: instances[idx].stop(), list(range(len(instances))))
