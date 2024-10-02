import yt.wrapper as yt

import yt.logger as logger


############################################################################


def _validate_bundle_exists(client, bundle):
    assert client.exists(
        "//sys/tablet_cell_bundles/{}".format(bundle)
    ), 'Tablet cell bundle "{}" does not exist'.format(bundle)


def _disable_in_memory_cell_balancer(client, dry_run, bundle):
    logger.info("Disabling in memory cell balancer")
    _validate_bundle_exists(client, bundle)
    attribute_path = yt.ypath_join(
        "//sys/tablet_cell_bundles",
        bundle,
        "@tablet_balancer_config/enable_in_memory_cell_balancer",
    )
    logger.info("yt set {} %false".format(attribute_path))
    if not dry_run:
        client.set(attribute_path, False)


def _set_tablet_balancer_schedule(client, dry_run, bundle, schedule):
    logger.info("Setting tablet balancer schedule")
    _validate_bundle_exists(client, bundle)
    attribute_path = yt.ypath_join(
        "//sys/tablet_cell_bundles",
        bundle,
        "@tablet_balancer_config/tablet_balancer_schedule",
    )
    logger.info('yt set {} "{}"'.format(attribute_path, schedule))
    if not dry_run:
        client.set(attribute_path, schedule)


def _set_journals_media(client, dry_run, bundle):
    logger.info("Setting journals media")
    _validate_bundle_exists(client, bundle)

    changelog_path = yt.ypath_join(
        "//sys/tablet_cell_bundles",
        bundle,
        "@options/changelog_primary_medium",
    )
    changelog_medium = "ssd_journals"

    snapshot_path = yt.ypath_join(
        "//sys/tablet_cell_bundles",
        bundle,
        "@options/snapshot_primary_medium",
    )
    snapshot_medium = "ssd_blobs"

    logger.info('yt set {} "{}"'.format(changelog_path, changelog_medium))
    logger.info('yt set {} "{}"'.format(snapshot_path, snapshot_medium))
    if not dry_run:
        client.set(changelog_path, changelog_medium)
        client.set(snapshot_path, snapshot_medium)


############################################################################


def create_tablet_balancer_schedule(hours):
    return "hours == {} && minutes == 0".format(hours)


def configure_db_bundle(
    yt_proxy,
    dry_run,
    bundle,
    per_cluster_tablet_balancer_schedule,
    tablet_balancer_schedule_hours=None,
):
    client = yt.YtClient(yt_proxy)

    logger.info('Configuring tablet cell bundle "{}"'.format(bundle))

    _disable_in_memory_cell_balancer(client, dry_run, bundle)

    tablet_balancer_schedule = None
    if tablet_balancer_schedule_hours is not None:
        assert (
            0 <= tablet_balancer_schedule_hours < 24
        ), "Tablet balancer scheduler hours must be in range [0, 24), but got {}".format(
            tablet_balancer_schedule_hours,
        )
        tablet_balancer_schedule = create_tablet_balancer_schedule(
            tablet_balancer_schedule_hours,
        )

    if tablet_balancer_schedule is None:
        tablet_balancer_schedule = per_cluster_tablet_balancer_schedule.get(yt_proxy)

    assert (
        tablet_balancer_schedule is not None
    ), 'Tablet balancer schedule is not specified and cannot be inferred from YT proxy "{}"'.format(
        yt_proxy
    )

    _set_tablet_balancer_schedule(client, dry_run, bundle, tablet_balancer_schedule)

    _set_journals_media(client, dry_run, bundle)
