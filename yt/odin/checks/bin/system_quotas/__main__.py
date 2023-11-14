from collections import defaultdict

from yt_odin_checks.lib.system_quotas import get_quota_holders_resource_usage
from yt_odin_checks.lib.system_quotas import get_resource_usage_in_precents
from yt_odin_checks.lib.system_quotas import is_resource_exhausted
from yt_odin_checks.lib.system_quotas import get_cluster_name
from yt_odin_checks.lib.system_quotas import get_not_so_critical_quota_holders
from yt_odin_checks.lib.system_quotas import get_quota_holder_threshold
from yt_odin_checks.lib.system_quotas import build_link_to_account
from yt_odin_checks.lib.system_quotas import build_link_to_bundle
from yt_odin_checks.lib.check_runner import main


class BaseConfig:
    DEFAULT_THRESHOLD = 85
    CUSTOM_THRESHOLDS = {}
    FLAT_RESOURCES = []
    SUBKEY_RESOURCES = []
    NOT_SO_CRITICAL_HOLDERS = set()
    NOT_SO_CRITICAL_HOLDERS_BY_CLUSTER = {}
    THRESHOLD_TIME_PERIOD_OVERRIDES = {}
    CLUSTERS_WITHOUT_TABLET_RESOURCES = []
    TABLET_RESOURCES = ["tablet_count", "tablet_static_memory"]

    @classmethod
    def get_flat_resources(cls, cluster):
        if cluster in cls.CLUSTERS_WITHOUT_TABLET_RESOURCES:
            return cls.FLAT_RESOURCES
        else:
            return cls.FLAT_RESOURCES + cls.TABLET_RESOURCES


class AccountConfig(BaseConfig):
    FLAT_RESOURCES = ["chunk_count", "node_count"]

    SUBKEY_RESOURCES = ["disk_space_per_medium"]

    ALL_POSSIBLE_HOLDERS = [
        "sys", "tmp", "intermediate", "tmp_files", "tmp_jobs", "yt-skynet-m5r",
        "operations_archive", "cron_compression", "clickhouse-kolkhoz", "yp",
        "yt-integration-tests", "logfeller-yt",
    ]

    # These accounts should lead only to partially available (yellow) state on all clusters
    NOT_SO_CRITICAL_HOLDERS = {
        # This account contains only logs, in worst-case scenario we
        # will just lose some night activity history.
        "clickhouse-kolkhoz",
    }

    # These accounts should lead only to partially available (yellow) state on specified clusters
    NOT_SO_CRITICAL_HOLDERS_BY_CLUSTER = {
        "hahn": {
            # YP Backups
            "yp",
        },
        "arnold": {
            # YP Backups
            "yp",
        },
        "socrates": {
            # Account for tests
            "yp",
        }
    }

    # For some accounts threshold depends on the time
    THRESHOLD_TIME_PERIOD_OVERRIDES = {
        "logfeller-yt": {
            "start_time": "00:00:00",
            "end_time": "10:00:00",
            "threshold": 95,
        },
        "yt-integration-tests": {
            "start_time": "00:00:00",
            "end_time": "10:00:00",
            "threshold": 95,
        },
    }

    # Per-account tablet resource accounting is disabled.
    CLUSTERS_WITHOUT_TABLET_RESOURCES = {
        "zeno",
        "hume",
        "freud",
        "seneca-sas",
        "seneca-vla",
        "arnold",
    }


class BundleConfig(BaseConfig):
    ALL_POSSIBLE_HOLDERS = ["sys", "sys_blobs", "sys_operations"]

    # Per-bundle tablet resource accounting is not enabled.
    CLUSTERS_WITHOUT_TABLET_RESOURCES = {
        "yp-iva",
        "yp-man",
        "yp-man-pre",
        "yp-myt",
        "yp-sas",
        "yp-sas-test",
        "yp-vla",
        "yp-vlx",
        "yp-klg",
        "yp-xdc",
    }


class AccountFacade:
    build_link_to_quota_holder = build_link_to_account
    holder_map_name = "accounts"
    holder_type = "Account"


class BundleFacade:
    build_link_to_quota_holder = build_link_to_bundle
    holder_map_name = "tablet_cell_bundles"
    holder_type = "Bundle"


def do_run_check(yt_client, logger, states, facade, config):
    cluster = get_cluster_name(yt_client)
    not_so_critical_quota_holders = get_not_so_critical_quota_holders(
        cluster,
        config.NOT_SO_CRITICAL_HOLDERS,
        config.NOT_SO_CRITICAL_HOLDERS_BY_CLUSTER)

    resource_usage_info = get_quota_holders_resource_usage(
        yt_client,
        facade.holder_map_name,
        config.ALL_POSSIBLE_HOLDERS)

    exhausted_resources = defaultdict(lambda: defaultdict(float))
    for holder, resources_info in resource_usage_info.items():
        threshold = get_quota_holder_threshold(
            holder,
            config.DEFAULT_THRESHOLD,
            config.CUSTOM_THRESHOLDS,
            config.THRESHOLD_TIME_PERIOD_OVERRIDES)
        for resource in config.SUBKEY_RESOURCES:
            for subkey in resources_info.limits.get(resource, {}):
                limit = resources_info.limits.get(resource, {}).get(subkey, 0)
                usage = resources_info.usage.get(resource, {}).get(subkey, 0)
                used_percent = get_resource_usage_in_precents(usage, limit)
                if is_resource_exhausted(used_percent, threshold):
                    exhausted_resources[holder]["{}/{}".format(resource, subkey)] = used_percent

        for resource in config.get_flat_resources(cluster):
            limit = resources_info.limits.get(resource, 0)
            usage = resources_info.usage.get(resource, 0)
            used_percent = get_resource_usage_in_precents(usage, limit)
            if is_resource_exhausted(used_percent, threshold):
                exhausted_resources[holder][resource] = used_percent

    if exhausted_resources:
        descriptions = []
        for holder, resources in exhausted_resources.items():
            for resource, used_percent in resources.items():
                log_msg = '{} "{}": {} exhausted for {:04.2f}%, link: {}'.format(
                    facade.holder_type,
                    holder,
                    resource,
                    used_percent,
                    facade.build_link_to_quota_holder(cluster, holder))
                logger.info(log_msg)
                descriptions.append(log_msg)
        if all(holder in not_so_critical_quota_holders for holder in exhausted_resources):
            state = states.PARTIALLY_AVAILABLE_STATE
        else:
            state = states.UNAVAILABLE_STATE
        return state, descriptions

    return states.FULLY_AVAILABLE_STATE, []


def run_check(yt_client, logger, options, states):
    aggregated_state = states.FULLY_AVAILABLE_STATE
    aggregated_descriptions = []

    def combine_result(check_state, check_descriptions):
        nonlocal aggregated_state
        nonlocal aggregated_descriptions
        aggregated_state = min(aggregated_state, check_state)
        aggregated_descriptions += check_descriptions

    combine_result(*do_run_check(yt_client, logger, states, AccountFacade, AccountConfig))
    combine_result(*do_run_check(yt_client, logger, states, BundleFacade, BundleConfig))

    if aggregated_state == states.FULLY_AVAILABLE_STATE:
        return aggregated_state
    else:
        return aggregated_state, "; ".join(aggregated_descriptions)


if __name__ == "__main__":
    main(run_check)
