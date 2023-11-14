from __future__ import unicode_literals

from yt_odin_checks.lib.check_runner import main
from yt_odin_checks.lib.tablet_cell_helpers import (
    get_current_cell_attributes,
    BundlesLimitsViolationTracker,
    run_and_log_time
)

import functools
from collections import defaultdict


CELL_MESSAGES_SAMPLE_SIZE = 20
CHECK_DISK_SPACE_CELL_COUNT_LIMIT = 150

MAX_CHANGELOG_DISK_SPACE_TO_SKIP = 2 ** 15  # 32 Kb
MAX_SKIPPED_CHANGELOG_COUNT = 12

SNAPSHOT_HEALTH_MAX_DELTA = 50
# At least until we will learn to pessimize moving to nodes that are about to be decommissioned.
SNAPSHOT_HEALTH_YELLOW_DELTA = 12

MUTE_BUNDLE_MARK = "mute_tablet_cell_snapshots_check"


def run_check(yt_client, logger, options, states):
    yt_client.config["proxy"]["request_timeout"] = 30000

    def parse_cell_attributes(cell_id, attributes):
        max_changelog_ids[cell_id] = attributes["max_changelog_id"]
        max_snapshot_ids[cell_id] = attributes["max_snapshot_id"]
        cell_to_bundle[cell_id] = attributes["tablet_cell_bundle"]

    def get_nonskipped_changelog_count_by_cell(cell_ids_to_check):
        changelog_count_by_cell = defaultdict(int)

        if not cell_ids_to_check:
            return changelog_count_by_cell

        changelog_disk_space_by_cell_id = {}
        for cell_id in cell_ids_to_check:
            changelog_disk_space_by_cell_id[cell_id] = \
                yt_batch_client.list("//sys/tablet_cells/{}/changelogs".format(cell_id), attributes=["resource_usage"])

        run_and_log_time(
            functools.partial(yt_batch_client.commit_batch),
            "fetching changelogs disk space usage over {} cells".format(len(cell_ids_to_check)),
            logger)

        for cell_id in changelog_disk_space_by_cell_id:
            results = changelog_disk_space_by_cell_id[cell_id].get_result()
            considered_ids = set(range(max_snapshot_ids[cell_id] + 1, max_changelog_ids[cell_id] + 1))
            for result in results:
                if int(result) in considered_ids:
                    if result.attributes["resource_usage"]["disk_space"] > MAX_CHANGELOG_DISK_SPACE_TO_SKIP:
                        changelog_count_by_cell[cell_id] += 1

            changelog_count_by_cell[cell_id] = max(
                changelog_count_by_cell[cell_id],
                max_changelog_ids[cell_id] - max_snapshot_ids[cell_id] - MAX_SKIPPED_CHANGELOG_COUNT)

        return changelog_count_by_cell

    # Skip bundle if it violates snapshot account limits or is explicitly muted.
    def fetch_violating_bundles(bundles):
        bundles_with_violated_limits = limits_violation_tracker.filter_bundles_with_violated_limits(bundles)
        violating_bundles = set()
        for bundle, bundle_violations in bundles_with_violated_limits.items():
            if MUTE_BUNDLE_MARK in bundle_violations:
                logger.warning("Skipping bundle '{}' that is manually muted".format(bundle))
                violating_bundles.add(bundle)
            elif "snapshot" in bundle_violations:
                logger.warning("Skipping bundle '{}' that has violated snapshot account '{}' resources: {}".format(
                    bundle,
                    limits_violation_tracker.get_bundle_account(bundle),
                    bundle_violations["snapshot"]))
                violating_bundles.add(bundle)
            elif "changelog" in bundle_violations:
                logger.warning("Skipping bundle '{}' that has violated changelog account '{}' resources: {}".format(
                    bundle,
                    limits_violation_tracker.get_bundle_account(bundle),
                    bundle_violations["changelog"]))
                violating_bundles.add(bundle)

        return violating_bundles

    yt_batch_client = yt_client.create_batch_client(max_batch_size=200)

    max_delta = options.get("max_delta", SNAPSHOT_HEALTH_MAX_DELTA)
    yellow_delta = options.get("yellow_delta", SNAPSHOT_HEALTH_YELLOW_DELTA)
    message = "SNAPSHOT_HEALTH:"
    exit_code = 0

    limits_violation_tracker = BundlesLimitsViolationTracker(
        yt_client, yt_batch_client, logger, "snapshot", MUTE_BUNDLE_MARK, "tablet_cell_snapshots")

    max_changelog_ids = {}
    max_snapshot_ids = {}
    cell_to_bundle = {}
    get_current_cell_attributes(
        yt_client,
        logger,
        ["max_changelog_id", "max_snapshot_id", "tablet_cell_bundle"],
        parse_cell_attributes,
        "tablet_cell_snapshots")

    delta_by_bundle = defaultdict(int)

    cell_ids_exceeding_delta = []
    cell_ids_exceeding_max_delta_by_bundle = defaultdict(list)
    cell_ids_exceeding_yellow_delta_by_bundle = defaultdict(list)

    for cell_id in max_changelog_ids:
        bundle = cell_to_bundle[cell_id]
        max_changelog_id = max_changelog_ids[cell_id]
        max_snapshot_id = max_snapshot_ids[cell_id]
        # We do not check max_snapshot_id here, because it indicates that we are failing to build any snapshot.
        if max_changelog_id == -1:
            continue

        delta = max_changelog_id - max_snapshot_id
        if delta >= yellow_delta:
            delta_by_bundle[bundle] += delta
            exit_code = 1
            if delta >= max_delta:
                cell_ids_exceeding_max_delta_by_bundle[bundle].append(cell_id)
            else:
                cell_ids_exceeding_yellow_delta_by_bundle[bundle].append(cell_id)
            cell_ids_exceeding_delta.append(cell_id)

    if exit_code == 0:
        return states.FULLY_AVAILABLE_STATE, "OK"

    bundles_to_check = list(set(
        list(cell_ids_exceeding_max_delta_by_bundle.keys()) +
        list(cell_ids_exceeding_yellow_delta_by_bundle.keys())))
    violating_bundles = fetch_violating_bundles(bundles_to_check)

    skipped_delta_total = 0
    skipped_cell_count_by_bundle = defaultdict(int)

    for bundle, cell_ids in cell_ids_exceeding_max_delta_by_bundle.items():
        if bundle in violating_bundles:
            skipped_cell_count_by_bundle[bundle] = len(cell_ids)
            skipped_delta_total += delta_by_bundle[bundle]
        else:
            exit_code = 2

    nonskipped_changelog_count_by_cell = defaultdict(int)
    if exit_code == 1:
        cell_ids_to_check = []
        for bundle, cell_ids in cell_ids_exceeding_yellow_delta_by_bundle.items():
            if bundle in violating_bundles:
                if bundle not in skipped_cell_count_by_bundle:
                    skipped_delta_total += delta_by_bundle[bundle]
                skipped_cell_count_by_bundle[bundle] += len(cell_ids)
            else:
                cell_ids_to_check += cell_ids

        if len(cell_ids_to_check) > CHECK_DISK_SPACE_CELL_COUNT_LIMIT:
            logger.warning("Will not fetch changelog disk space delta due to large amount of cells: {} > {}".format(
                len(cell_ids_to_check),
                CHECK_DISK_SPACE_CELL_COUNT_LIMIT))
            exit_code = 2
        else:
            nonskipped_changelog_count_by_cell = get_nonskipped_changelog_count_by_cell(cell_ids_to_check)
            for changelog_count in nonskipped_changelog_count_by_cell.values():
                if changelog_count >= yellow_delta:
                    exit_code = 2

    if skipped_cell_count_by_bundle:
        logger.warning("{} failed cells skipped from bundles {}. Skipped average delta is: {}".format(
            sum(skipped_cell_count_by_bundle.values()),
            skipped_cell_count_by_bundle.keys(),
            skipped_delta_total / sum(skipped_cell_count_by_bundle.values())))

    cell_messages = []
    for cell_id in cell_ids_exceeding_delta:
        delta = max_changelog_ids[cell_id] - max_snapshot_ids[cell_id]
        nonskipped_changelog_delta = nonskipped_changelog_count_by_cell.get(cell_id, delta)

        error_message = "Tablet cell '{}' from bundle '{}', changelog_delta: {}, changelog_total_delta: {};".format(
            cell_id,
            cell_to_bundle[cell_id],
            nonskipped_changelog_delta,
            delta)
        cell_messages.append({
            "message": error_message,
            "delta": nonskipped_changelog_delta,
            "is_bundle_skipped": int(cell_to_bundle[cell_id] in violating_bundles)})

    cell_messages.sort(key=lambda x: (x["is_bundle_skipped"], -x["delta"]))
    cell_message_count = min(CELL_MESSAGES_SAMPLE_SIZE, len(cell_messages))

    message = "SNAPSHOT_HEALTH"
    if cell_message_count == len(cell_messages):
        message += " ({} cells): ".format(cell_message_count)
    else:
        message += " ({} out of {} cells):".format(cell_message_count, len(cell_messages))
    logger.info(message)
    for cell_message in cell_messages[:cell_message_count]:
        logger.info(cell_message["message"])
        message += " " + cell_message["message"]

    if exit_code == 1:
        return states.PARTIALLY_AVAILABLE_STATE, message
    elif exit_code == 2:
        return states.UNAVAILABLE_STATE, message


if __name__ == "__main__":
    main(run_check)
