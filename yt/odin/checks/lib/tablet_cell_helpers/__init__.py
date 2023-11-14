from __future__ import unicode_literals

from yt.common import datetime_to_string, date_string_to_timestamp

import functools
from datetime import datetime, timedelta
from collections import defaultdict


# This is an internal bundle attribute that provides information on
# currently violated resources of an account associated with the bundle.
ACCOUNT_VIOLATION_ATTRIBUTE = "{entity}_account_violated_resource_limits"

# This is a custom attribute to keep track of a violation in the past to perform fading out.
VIOLATION_INSTANT_ATTRIBUTE = "{entity}_account_resource_limits_violated_at"

VIOLATION_MARK_DELETE_IN_MINUTES = 60

ENTITY_TO_LIMIT_VIOLATION_TIMEOUT_MINUTES = {
    "changelog": 3,
    "snapshot": 30
}


def run_and_log_time(partial, description, logger):
    start = datetime.now()
    result = partial()
    delta = datetime.now() - start
    logger.info("{}.{} seconds spent on {}.".format(delta.seconds, delta.microseconds, description))
    return result


def get_current_cell_attributes(yt_client, logger, attributes, parse_attributes, check_name):
    assert check_name in [
        "tablet_cell_gossip",
        "tablet_cells",
        "tablet_cell_snapshots",
        "chaos_cells",], \
        "Unknown check name {}".format(check_name)
    if check_name == "chaos_cells":
        cells_path = "//sys/chaos_cells"
    else:
        cells_path = "//sys/tablet_cells"
    cells = run_and_log_time(
        functools.partial(
            yt_client.get,
            cells_path,
            attributes=attributes),
        "fetching cell attributes: {}".format(attributes),
        logger)
    for cell_id, fetched_attrs in cells.items():
        parse_attributes(cell_id, fetched_attrs.attributes)


class BundlesLimitsViolationTracker(object):
    def __init__(self, yt_client, yt_batch_client, logger, entity, mute_mark, check_name):
        self._yt_client = yt_client
        self._yt_batch_client = yt_batch_client
        self._logger = logger

        assert entity in ["snapshot", "changelog"], "Unknown entity {}".format(entity)
        assert mute_mark in [
            "mute_tablet_cell_gossip_check",
            "mute_tablet_cells_check",
            "mute_tablet_cell_snapshots_check",
            "mute_chaos_cells_check",
        ], "Unknow mute mark {}".format(mute_mark)
        assert check_name in [
            "tablet_cell_gossip",
            "tablet_cells",
            "tablet_cell_snapshots",
            "chaos_cells",
        ], "Unknown check name {}".format(check_name)

        self._entity = entity
        self._mute_mark = mute_mark
        self._check_name = check_name

        self._initialized = False

        self._bundles_limits_inspected = defaultdict(list)
        self._violated_bundles = defaultdict(lambda: defaultdict(list))
        self._bundles_options = {}
        self._muted_bundles = set()

    def _initialize(self):
        assert not self._initialized
        self._initialized = True

        attributes = [
            "options",
            self._mute_mark,
            self._get_violation_instant_attribute("snapshot")]
        if self._entity == "changelog":
            attributes.append(self._get_violation_instant_attribute("changelog"))

        cell_bundles = self._yt_client.get(self._get_bundles_path(),
                                           attributes=attributes)
        for bundle in cell_bundles.keys():
            self._bundles_options[bundle] = cell_bundles[bundle].attributes["options"]
            if cell_bundles[bundle].attributes.get(self._mute_mark, None):
                self._muted_bundles.add(bundle)

            def _process_violation_mark(entity):
                violation_instant_attribute = self._get_violation_instant_attribute(entity)
                violated_at = cell_bundles[bundle].attributes.get(violation_instant_attribute, None)
                if violated_at:
                    violated_at_unix = date_string_to_timestamp(violated_at)
                    timeout = datetime.utcfromtimestamp(violated_at_unix) +\
                        timedelta(minutes=ENTITY_TO_LIMIT_VIOLATION_TIMEOUT_MINUTES[self._entity])
                    mark_delete_timeout = datetime.utcfromtimestamp(violated_at_unix) +\
                        timedelta(minutes=VIOLATION_MARK_DELETE_IN_MINUTES)
                    if datetime.utcnow() < timeout:
                        self._violated_bundles[bundle][entity].append("violation_fade_out")
                        self._logger.info("Bundle '{}' has {} violation mark active till: {}".format(bundle, entity, timeout))
                    elif datetime.utcnow() > mark_delete_timeout:
                        mark_path = "{}/{}/@{}".format(
                            self._get_bundles_path(),
                            bundle,
                            violation_instant_attribute)
                        self._logger.info("Bundle '{}' {} violation mark is deleted: {}".format(bundle, entity, mark_path))
                        self._yt_client.remove(mark_path)

            _process_violation_mark("snapshot")
            if self._entity == "changelog":
                _process_violation_mark("changelog")

    def _put_violation_mark(self, bundle, entity):
        violation_instant_attribute = self._get_violation_instant_attribute(entity)
        self._yt_client.set("{}/{}/@{}".format(
                            self._get_bundles_path(),
                            bundle,
                            violation_instant_attribute),
                            datetime_to_string(datetime.utcnow()))

    def _get_violation_instant_attribute(self, entity):
        return VIOLATION_INSTANT_ATTRIBUTE.format(entity=entity)

    def _get_account_violation_attribute(self, entity):
        return ACCOUNT_VIOLATION_ATTRIBUTE.format(entity=entity)

    def _get_bundles_path(self):
        if self._check_name == "chaos_cells":
            return "//sys/chaos_cell_bundles"
        else:
            return "//sys/tablet_cell_bundles"

    def get_bundle_account(self, bundle):
        return self._bundles_options[bundle]["{}_account".format(self._entity)]

    def filter_bundles_with_violated_limits(self, bundles_to_check):
        if not self._initialized:
            self._initialize()

        if all([True if (self._entity in self._bundles_limits_inspected.get(b, ()))
                else False
                for b in bundles_to_check]):
            return self._violated_bundles

        bundle_account_violated_resources = defaultdict(dict)

        for bundle in bundles_to_check:
            if bundle in self._muted_bundles:
                continue

            bundle_account_violated_resources[bundle]["snapshot"] = self._yt_batch_client.get(
                "{}/{}/@{}".format(
                    self._get_bundles_path(),
                    bundle,
                    self._get_account_violation_attribute("snapshot")))

            if self._entity == "changelog":
                bundle_account_violated_resources[bundle]["changelog"] = self._yt_batch_client.get(
                    "{}/{}/@{}".format(
                        self._get_bundles_path(),
                        bundle,
                        self._get_account_violation_attribute("changelog")))

        run_and_log_time(
            functools.partial(self._yt_batch_client.commit_batch),
            "fetching violated account limits",
            self._logger)

        for bundle in bundles_to_check:
            self._bundles_limits_inspected[bundle].append(self._entity)

            if bundle in self._muted_bundles:
                self._violated_bundles[bundle][self._mute_mark]
                continue

            for entity, account_violated_resources in bundle_account_violated_resources[bundle].items():
                if not account_violated_resources.is_ok():
                    self._logger.error("Error occurred while fetching violated account limits: {}".format(
                        str(account_violated_resources.get_error())))
                    self._violated_bundles.clear()
                    return self._violated_bundles

                account_violated_resources = account_violated_resources.get_result()

                medium = self._bundles_options[bundle]["{}_primary_medium".format(entity)]
                if medium not in account_violated_resources["disk_space_per_medium"]:
                    misconfiguration_reason = "medium {} that is missing in account limits".format(medium)
                    self._logger.warning("Bundle '{}' seems to be misconfigured due to {}, hence it's intentionally not skipped".format(
                        bundle,
                        misconfiguration_reason))
                    self._violated_bundles.clear()
                    break

                violated_resources = []

                if account_violated_resources["disk_space_per_medium"][medium]:
                    violated_resources.append("disk_space in {}".format(medium))

                if account_violated_resources["node_count"]:
                    violated_resources.append("node_count")

                if account_violated_resources["chunk_count"]:
                    violated_resources.append("chunk_count")

                if violated_resources:
                    self._put_violation_mark(bundle, entity)
                    self._violated_bundles[bundle][entity].extend(violated_resources)

        self._logger.info("Violated bundles: {}".format(list(self._violated_bundles.keys())))
        return self._violated_bundles
