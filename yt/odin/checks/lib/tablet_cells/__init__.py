from __future__ import unicode_literals

from yt_odin_checks.lib.tablet_cell_helpers import (
    run_and_log_time,
    get_current_cell_attributes,
    BundlesLimitsViolationTracker
)

import functools
import itertools
from collections import deque, defaultdict
from tabulate import tabulate

from yt.wrapper import YtResponseError


HISTORY_PATH_PATTERN = "//sys/admin/odin/{check_name}"
HISTORY_DEPTH = 10

MAX_MULTICELL_CHECK_CELL_COUNT = 8
FAILED_CELLS_SAMPLE_SIZE = 20

MUTE_BUNDLE_MARK_PATTERN = "mute_{check_name}_check"

# This indicates number of minutes without good health (per cell) to mark check state as critical.
CRITICAL_STATE_LIMIT = {
    "failed": 1,
    "degraded": 5,
    "initializing": 5
}


def run_check_impl(yt_client, logger, options, states, check_name):
    assert check_name in ["tablet_cells", "tablet_cell_gossip", "chaos_cells"]

    HISTORY_PATH = HISTORY_PATH_PATTERN.format(check_name=check_name)
    MUTE_BUNDLE_MARK = MUTE_BUNDLE_MARK_PATTERN.format(check_name=check_name)

    yt_client.config["proxy"]["request_timeout"] = 30000

    def parse_cell_attributes(cell_id, attributes):
        cell_to_bundle[cell_id] = attributes["tablet_cell_bundle"]

        if check_name in ["tablet_cells", "chaos_cells"]:
            health_value = attributes.get("local_health")
        else:
            health_value = attributes["status"]["health"]

        cell_to_state[cell_id] = health_value
        cell_states[health_value].append(cell_id)

    def get_previous_cell_states():
        if yt_client.exists(HISTORY_PATH):
            history = yt_client.get(HISTORY_PATH)
            if '__iter__' in dir(history):
                return list(history)
        return []

    def save_current_cell_states(current_state, history_state):
        queue = deque(history_state)
        while len(queue) >= HISTORY_DEPTH:
            queue.pop()
        queue.appendleft(current_state)
        if not yt_client.exists(HISTORY_PATH):
            yt_client.create("document", HISTORY_PATH)

        run_and_log_time(
            functools.partial(yt_client.set, HISTORY_PATH, list(queue)),
            "setting state document",
            logger)

    def filter_bundles_with_violated_changelogs(bundles):
        bundles_with_violated_limits = limits_violation_tracker.filter_bundles_with_violated_limits(bundles)
        for bundle, bundle_violations in bundles_with_violated_limits.items():
            if MUTE_BUNDLE_MARK in bundle_violations:
                logger.warning("Skipping bundle '{}' that is manually muted".format(bundle))
                skipped_bundles.add(bundle)
            elif "changelog" in bundle_violations:
                logger.warning("Skipping bundle '{}' that has violated changelog account '{}' resources: {}".format(
                    bundle,
                    limits_violation_tracker.get_bundle_account(bundle),
                    bundle_violations["changelog"]))
                skipped_bundles.add(bundle)
            elif "snapshot" in bundle_violations:
                logger.warning("Skipping bundle '{}' that has violated snapshot account '{}' resources: {}".format(
                    bundle,
                    limits_violation_tracker.get_bundle_account(bundle),
                    bundle_violations["snapshot"]))
                skipped_bundles.add(bundle)

        return [bundle for bundle in bundles if bundle not in skipped_bundles]

    def log_multicell_status():
        multicell_status = {}
        for cell_id in non_good_cell_ids[:MAX_MULTICELL_CHECK_CELL_COUNT]:
            assert check_name == "tablet_cell_gossip"
            multicell_status[cell_id] = yt_batch_client.get("//sys/tablet_cells/{}/@multicell_status".format(cell_id))
        run_and_log_time(
            functools.partial(yt_batch_client.commit_batch),
            "fetching tablet cells multicell status",
            logger)

        cell_results = {}
        master_cell_ids = set()
        for cell_id in multicell_status.keys():
            result = multicell_status[cell_id].get_result()
            if result is not None:
                result = {tag: value["health"] for tag, value in result.items()}
                cell_results[cell_id] = result
                master_cell_ids.update(result.keys())
            else:
                logger.warning("TABLET_CELL_MULTICELL_HEALTH result is None for cell {}".format(cell_id))
        master_cell_ids = sorted(master_cell_ids)

        table_data = []
        for cell_id, cell_result in cell_results.items():
            row = [cell_id]
            for master_cell_id in master_cell_ids:
                row.append(cell_result.get(master_cell_id, None))
            table_data.append(row)

        table = tabulate(table_data, headers=[] + list(master_cell_ids), tablefmt="grid")
        logger.info("TABLET_CELL_MULTICELL_HEALTH\n{}".format(table))

    def log_failed_cells(yt_batch_client):
        failed_cells = critical_cells if critical_cells else non_good_cells

        failed_without_violation = {}
        for health, cell_ids in failed_cells.items():
            unfixed_cells = [
                cell_id
                for cell_id in cell_ids
                if cell_to_bundle[cell_id] not in skipped_bundles]
            if unfixed_cells:
                failed_without_violation[health] = unfixed_cells

        description = "Critial " if critical_cells else "Worrying "
        if failed_without_violation:
            description += "non-violated "
            failed_cells = failed_without_violation

        sampled_cells = {health: cell_ids[:FAILED_CELLS_SAMPLE_SIZE] for health, cell_ids in failed_cells.items()}
        failed_cell_count = sum([len(cell_ids) for cell_ids in failed_cells.values()])
        sampled_failed_cell_count = sum([len(cell_ids) for cell_ids in sampled_cells.values()])

        if sampled_failed_cell_count == failed_cell_count:
            description += "tablet cells ({} cells):\n{}".format(
                failed_cell_count,
                sampled_cells)
        else:
            description += "tablet cells ({} out of {}):\n{}".format(
                sampled_failed_cell_count,
                failed_cell_count,
                sampled_cells)

        cell_to_peers_info = {}
        for _, cell_ids in sampled_cells.items():
            for cell_id in cell_ids:
                cell_to_peers_info[cell_id] = yt_batch_client.get(
                    "#{}/@peers".format(cell_id))

        run_and_log_time(
            functools.partial(yt_batch_client.commit_batch),
            "fetching failed cells peer info",
            logger)

        if sampled_failed_cell_count == failed_cell_count:
            logger.info("Failed cells info ({} cells):".format(
                failed_cell_count))
        else:
            logger.info("Sampled failed cells info ({} out of {}):".format(
                sampled_failed_cell_count,
                failed_cell_count))

        for health, cell_ids in sampled_cells.items():
            for cell_id in cell_ids:
                peers_info = cell_to_peers_info[cell_id]
                if not peers_info.is_ok():
                    raise YtResponseError(peers_info.get_error())
                peers_info = peers_info.get_result()
                logger.info("{}: {}".format(cell_id, peers_info))

    yt_batch_client = yt_client.create_batch_client(max_batch_size=200)

    message = ""
    skipped_bundles = set()

    limits_violation_tracker = BundlesLimitsViolationTracker(
        yt_client, yt_batch_client, logger, "changelog", MUTE_BUNDLE_MARK, check_name)

    cell_states = defaultdict(list)
    cell_to_state = {}
    cell_to_bundle = {}
    attributes_to_fetch = ["tablet_cell_bundle"]
    if check_name in ["tablet_cells", "chaos_cells"]:
        attributes_to_fetch.append("local_health")
    else:
        attributes_to_fetch.append("status")
    get_current_cell_attributes(
        yt_client,
        logger,
        attributes_to_fetch,
        parse_cell_attributes,
        check_name)

    previous_cell_states = get_previous_cell_states()

    non_good_cells = {health: cell_ids for health, cell_ids in cell_states.items() if health != "good"}
    non_good_cell_ids = list(itertools.chain.from_iterable(non_good_cells.values()))
    critical_cells = defaultdict(list)

    cell_count_by_state = {health: len(cell_ids) for health, cell_ids in cell_states.items()}
    non_good_cell_count_by_state = {health: len(cell_ids) for health, cell_ids in non_good_cells.items()}

    if len(non_good_cells) == 0:
        logger.info("All tablet cells are in a good state: {}".format(cell_count_by_state))
        save_current_cell_states(non_good_cells, previous_cell_states)
        return states.FULLY_AVAILABLE_STATE, "OK"

    logger.info("Tablet cells health summary: {}".format(cell_count_by_state))

    # Default is WARN, we can pessimize this to CRIT later.
    non_good_bundles = list(set(map(lambda cell_id: cell_to_bundle[cell_id], non_good_cell_ids)))
    message = "TABLET_CELLS_HEALTH: WARN - {} from bundles {} - I will watch for them"\
              .format(non_good_cell_count_by_state, non_good_bundles)
    exit_code = 1

    if check_name == "tablet_cell_gossip":
        log_multicell_status()

    if len(previous_cell_states) > 0:
        # Sentinel.
        previous_cell_states.append({})
        current_cell_ids = set(non_good_cell_ids)
        last_good_state_by_cell = {}
        for i, previous_state in enumerate(previous_cell_states):
            previous_cell_ids = set(itertools.chain.from_iterable(previous_state.values()))
            previous_good_cell_ids = current_cell_ids.difference(previous_cell_ids)
            for cell_id in previous_good_cell_ids:
                last_good_state_by_cell[cell_id] = i
                current_cell_ids.remove(cell_id)

        critical_bundles = set()
        for cell_id, last_good_state in last_good_state_by_cell.items():
            health = cell_to_state[cell_id]
            if last_good_state >= CRITICAL_STATE_LIMIT.get(health, 0):
                critical_cells[health].append(cell_id)
                critical_bundles.add(cell_to_bundle[cell_id])

        unfixed_bundles = filter_bundles_with_violated_changelogs(critical_bundles)

        if unfixed_bundles:
            exit_code = 2

            unfixed_cell_count_by_state = defaultdict(int)
            for health, cell_ids in critical_cells.items():
                unfixed_cell_ids = [cell_id for cell_id in cell_ids if cell_to_bundle[cell_id] in unfixed_bundles]
                unfixed_cell_count_by_state[health] = len(unfixed_cell_ids)

            message = "TABLET_CELLS_HEALTH: CRIT - {} from bundles {} are unfixed from previous checks;".format(
                dict(unfixed_cell_count_by_state),
                unfixed_bundles,
            )

            warn_cell_count_by_state = {}
            for health, count in non_good_cell_count_by_state.items():
                critical_count = unfixed_cell_count_by_state.get(health, 0)
                if count > critical_count:
                    warn_cell_count_by_state[health] = count - critical_count
            if warn_cell_count_by_state:
                message += " WARN - {}".format(warn_cell_count_by_state)

    logger.info(message)
    log_failed_cells(yt_batch_client)

    save_current_cell_states(non_good_cells, previous_cell_states)

    if exit_code == 1:
        return states.PARTIALLY_AVAILABLE_STATE, message
    elif exit_code == 2:
        return states.UNAVAILABLE_STATE, message
    else:
        raise RuntimeError("Unexpected exit code '{}' found".format(exit_code))
