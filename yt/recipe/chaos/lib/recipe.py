from yt.wrapper import yson

from yt.recipe.basic.lib import recipe as basic_yt_recipe

from .chaos import configure_chaos, patch_timestamp_providers_configs_and_restart, DEFAULT_CHAOS_CONFIG

import argparse


def _create_tablet_cell_bundle(clusters, bundle_name, clock_cluster_tag):
    """Create separate tablet_cell_bundle with configured clock_cluster_tag.
    Usage of this bundle instead of "default" bundle is required for chaos.
    For replicated tables any bundle can be used, including "default" bundle,
    so it is not required to create a separate bundle."""

    attributes = {
        "name": bundle_name,
        "options": {
            "changelog_replication_factor": 1,
            "changelog_read_quorum": 1,
            "changelog_write_quorum": 1,
            "changelog_account": "sys",
            "snapshot_replication_factor": 1,
            "snapshot_account": "sys",
            "snapshot_primary_medium": "default",
            "changelog_primary_medium": "default",
        },
    }
    if clock_cluster_tag is not None:
        attributes["options"]["clock_cluster_tag"] = clock_cluster_tag

    for cluster in clusters.values():
        cluster.get_yt_client().create("tablet_cell_bundle", "//sys/tablet_cell/bundles", attributes=attributes)


def start(yt_cluster_factory, args, work_dir=None):
    """recipe entry point (start services)."""
    parser = argparse.ArgumentParser()

    parser.add_argument("--db-mode", choices=["replicated", "chaos"], default="replicated",
                        help="database mode")

    parser.add_argument("--tablet-cell-bundle-name",
                        help="name of additional tablet_cell_bundle to create, required with --db-mode=chaos")
    parsed_args, _ = parser.parse_known_args(args)

    if parsed_args.db_mode == "chaos" and not parsed_args.tablet_cell_bundle_name:
        parser.error("--db-mode=chaos requires --tablet-cell-bundle-name")

    if parsed_args.db_mode == "chaos":
        args.append("--config-patch")
        args.append(str(yson.dumps(DEFAULT_CHAOS_CONFIG).decode('utf-8')))

    clusters = basic_yt_recipe.start(yt_cluster_factory, args, work_dir=work_dir)

    clock_cluster_tag = None
    if parsed_args.db_mode == "chaos":
        patch_timestamp_providers_configs_and_restart(clusters)
        configure_chaos(clusters)

        primary_client = clusters["primary"].get_yt_client()
        clock_cluster_tag = primary_client.get("//sys/@primary_cell_tag")

    if parsed_args.tablet_cell_bundle_name:
        _create_tablet_cell_bundle(clusters, parsed_args.tablet_cell_bundle_name, clock_cluster_tag)

    return clusters


def stop(args):
    """recipe entry point (stop services)."""
    basic_yt_recipe.stop(args)
