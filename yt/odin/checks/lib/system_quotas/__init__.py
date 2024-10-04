from datetime import datetime
from collections import defaultdict

try:
    from yt_odin_checks.lib.yandex_helpers import get_account_link
    from yt_odin_checks.lib.yandex_helpers import get_bundle_link
except ImportError:
    get_account_link = lambda cluster, account: ""  # noqa
    get_bundle_link = lambda cluster, bundle: ""  # noqa


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
    build_link_to_quota_holder = get_account_link
    holder_map_name = "accounts"
    holder_type = "Account"


class BundleFacade:
    build_link_to_quota_holder = get_bundle_link
    holder_map_name = "tablet_cell_bundles"
    holder_type = "Bundle"


class Resources:
    limits = None
    usage = None

    def __str__(self):
        return "limits:{} usage:{}".format(self.limits, self.usage)


def get_resource_usage_in_precents(usage, limit):
    if usage == 0 or limit == 0:
        return 0
    return round(usage / float(limit) * 100, 2)


def is_resource_exhausted(used_percent, threshold):
    if used_percent >= threshold:
        return True
    return False


def fetch_quota_holders(client, holder_map_name, all_possible_holders):
    responses = {}
    batch_client = client.create_batch_client()
    for holder in all_possible_holders:
        responses[holder] = batch_client.exists(f"//sys/{holder_map_name}/{holder}")
    batch_client.commit_batch()
    return responses


def find_existing_quota_holders(client, holder_map_name, all_possible_holders):
    existing_holders = []
    for holder, response in fetch_quota_holders(client, holder_map_name, all_possible_holders).items():
        if response.get_result():
            existing_holders.append(holder)
    return existing_holders


def fetch_quota_holders_resource_usage(client, holder_map_name, holders):
    batch_client = client.create_batch_client()
    responses = defaultdict(Resources)
    for holder in holders:
        holder_path = f"//sys/{holder_map_name}/{holder}"
        responses[holder].usage = batch_client.get(f"{holder_path}/@resource_usage")
        responses[holder].limits = batch_client.get(f"{holder_path}/@resource_limits")
    batch_client.commit_batch()
    return responses


def parse_quota_holders_resource_usage(responses):
    result = defaultdict(Resources)
    for holder, response in responses.items():

        if not response.limits.is_ok():
            raise RuntimeError(response.limits.get_error())
        result[holder].limits = response.limits.get_result()

        if not response.usage.is_ok():
            raise RuntimeError(response.usage.get_error())
        result[holder].usage = response.usage.get_result()

    return result


def get_quota_holders_resource_usage(client, holder_map_name, all_possible_holders):
    holders = find_existing_quota_holders(client, holder_map_name, all_possible_holders)
    return parse_quota_holders_resource_usage(
        fetch_quota_holders_resource_usage(client, holder_map_name, holders))


def get_cluster_name(yt_client):
    return yt_client.config.get("proxy", {}).get("url", "").split('.')[0]


def _get_now_time():
    return datetime.now().time()


def get_quota_holder_threshold(quota_holder, default_threshold, custom_thresholds, time_period_overrides):
    def _strtime_to_time(time_str):
        return datetime.strptime(time_str, "%H:%M:%S").time()

    quota_holder_threshold = custom_thresholds.get(quota_holder, default_threshold)
    time_period_override = time_period_overrides.get(quota_holder, None)

    if time_period_override is not None:
        # Defaults here for safety
        start_time = _strtime_to_time(time_period_override.get("start_time", "00:00:00"))
        end_time = _strtime_to_time(time_period_override.get("end_time", "10:00:00"))

        now_time = _get_now_time()
        if start_time <= now_time <= end_time:
            quota_holder_threshold = time_period_override.get("threshold", quota_holder_threshold)

    return quota_holder_threshold


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


def run_check_impl(yt_client, logger, options, states):
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
