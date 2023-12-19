from collections import defaultdict

from yt_odin_checks.lib.system_quotas import get_quota_holders_resource_usage
from yt_odin_checks.lib.system_quotas import get_resource_usage_in_precents
from yt_odin_checks.lib.system_quotas import is_resource_exhausted
from yt_odin_checks.lib.system_quotas import get_cluster_name
from yt_odin_checks.lib.system_quotas import get_quota_holder_threshold
from yt_odin_checks.lib.system_quotas import build_link_to_account
from yt_odin_checks.lib.system_quotas import build_link_to_bundle
from yt_odin_checks.lib.check_runner import main


class BaseConfig:
    DEFAULT_THRESHOLD = 85
    FLAT_RESOURCES = []
    SUBKEY_RESOURCES = []
    TABLET_RESOURCES = ["tablet_count", "tablet_static_memory"]

    @classmethod
    def get_flat_resources(cls, enable_tablet_resource_validation):
        if enable_tablet_resource_validation:
            return cls.FLAT_RESOURCES + cls.TABLET_RESOURCES
        else:
            return cls.FLAT_RESOURCES


class AccountConfig(BaseConfig):
    FLAT_RESOURCES = ["chunk_count", "node_count"]
    SUBKEY_RESOURCES = ["disk_space_per_medium"]


class BundleConfig(BaseConfig):
    pass


class AccountFacade:
    build_link_to_quota_holder = build_link_to_account
    holder_map_name = "accounts"
    holder_type = "Account"


class BundleFacade:
    build_link_to_quota_holder = build_link_to_bundle
    holder_map_name = "tablet_cell_bundles"
    holder_type = "Bundle"


def do_run_check(yt_client, logger, states, options, facade, config):
    cluster = get_cluster_name(yt_client)
    not_so_critical_quota_holders = set(options.get("not_so_critical_names", [])) | \
        set(options.get("per_cluster_not_so_critical_names", []))

    resource_usage_info = get_quota_holders_resource_usage(
        yt_client,
        facade.holder_map_name,
        options["all_possible_names"])

    exhausted_resources = defaultdict(lambda: defaultdict(float))
    for holder, resources_info in resource_usage_info.items():
        threshold = get_quota_holder_threshold(
            holder,
            config.DEFAULT_THRESHOLD,
            options.get("custom_thresholds", {}),
            options.get("threshold_time_period_overrides", {}))
        for resource in config.SUBKEY_RESOURCES:
            for subkey in resources_info.limits.get(resource, {}):
                limit = resources_info.limits.get(resource, {}).get(subkey, 0)
                usage = resources_info.usage.get(resource, {}).get(subkey, 0)
                used_percent = get_resource_usage_in_precents(usage, limit)
                if is_resource_exhausted(used_percent, threshold):
                    exhausted_resources[holder]["{}/{}".format(resource, subkey)] = used_percent

        for resource in config.get_flat_resources(options["enable_tablet_resource_validation"]):
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

    combine_result(*do_run_check(yt_client, logger, states, options["accounts"], AccountFacade, AccountConfig))
    combine_result(*do_run_check(yt_client, logger, states, options["bundles"], BundleFacade, BundleConfig))

    if aggregated_state == states.FULLY_AVAILABLE_STATE:
        return aggregated_state
    else:
        return aggregated_state, "; ".join(aggregated_descriptions)


if __name__ == "__main__":
    main(run_check)
