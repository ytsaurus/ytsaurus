from collections import defaultdict
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, time
from typing import TypedDict, final

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
        return f"limits:{self.limits} usage:{self.usage}"


@final
class ScheduleOptionsDict(TypedDict):
    start_time: str
    end_time: str
    threshold: int | float


@final
class SubsystemOptionsDict(TypedDict):
    all_possible_names: list[str]
    default_schedule: list[ScheduleOptionsDict]
    not_so_critical_names: list[str]
    per_cluster_not_so_critical_names: list[str]
    threshold_time_period_overrides: dict[str, list[ScheduleOptionsDict]]
    enable_tablet_resource_validation: bool


@final
class OptionsDict(TypedDict):
    accounts: SubsystemOptionsDict
    bundles: SubsystemOptionsDict


def get_resource_usage_in_percents(usage, limit):
    if usage == 0 or limit == 0:
        return 0
    return round(usage / float(limit) * 100, 2)


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
    # Drop microseconds to avoid confusion when we compare now with 23:59:59
    # maximum value that can be specified in config
    return datetime.now().time().replace(microsecond=0)


def get_quota_holder_threshold(
    quota_holder: str,
    default_threshold: int,
    default_schedule: list[ScheduleOptionsDict],
    time_period_overrides: dict[str, list[ScheduleOptionsDict]],
    now_time: time
):
    def _strtime_to_time(time_str):
        return datetime.strptime(time_str, "%H:%M:%S").time()

    def get_threshold_from_schedule(schedule: ScheduleOptionsDict):
        for item in schedule:
            start_time = _strtime_to_time(item.get("start_time", "00:00:00"))
            end_time = _strtime_to_time(item.get("end_time", "23:59:59"))
            threshold = item["threshold"]
            if start_time > end_time:
                raise RuntimeError(f"misconfiguration: start_time cannot be after end_time: {start_time} > {end_time}")
            if start_time <= now_time <= end_time:
                return threshold

    quota_holder_threshold = None

    time_period_override = time_period_overrides.get(quota_holder, [])
    quota_holder_threshold = get_threshold_from_schedule(time_period_override)
    if quota_holder_threshold is not None:
        return quota_holder_threshold

    quota_holder_threshold = get_threshold_from_schedule(default_schedule)
    if quota_holder_threshold is not None:
        return quota_holder_threshold

    quota_holder_threshold = default_threshold
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
        def check_resource(key_list):
            def resolve_key_list(d):
                for k in key_list:
                    d = d.get(k, None)
                    if d is None:
                        return 0
                return d

            limit = resolve_key_list(resources_info.limits)
            usage = resolve_key_list(resources_info.usage)
            used_percent = get_resource_usage_in_percents(usage, limit)
            key_list_str = "/".join(key_list)
            logger.info(f"Resource {facade.holder_type} {holder} {key_list_str} usage: {used_percent}; threshold: {threshold}")
            if used_percent >= threshold:
                exhausted_resources[holder][key_list_str] = used_percent

        threshold = get_quota_holder_threshold(
            holder,
            config.DEFAULT_THRESHOLD,
            options.get("default_threshold_schedule", []),
            options.get("threshold_time_period_overrides", {}),
            now_time=_get_now_time())
        for resource in config.SUBKEY_RESOURCES:
            for subkey in resources_info.limits.get(resource, {}):
                check_resource([resource, subkey])

        for resource in config.get_flat_resources(options["enable_tablet_resource_validation"]):
            check_resource([resource])

    if exhausted_resources:
        descriptions = []
        for holder, resources in exhausted_resources.items():
            for resource, used_percent in resources.items():
                link = facade.build_link_to_quota_holder(cluster, holder)
                log_msg = f'{facade.holder_type} "{holder}": {resource} exhausted for {used_percent:04.2f}%, link: {link}'
                logger.info(log_msg)
                descriptions.append(log_msg)
        if all(holder in not_so_critical_quota_holders for holder in exhausted_resources):
            state = states.PARTIALLY_AVAILABLE_STATE
        else:
            state = states.UNAVAILABLE_STATE
        return state, descriptions

    return states.FULLY_AVAILABLE_STATE, []


def port_options(logger, options: OptionsDict, key: str) -> SubsystemOptionsDict:
    """
    Port options from old format to new.
    """
    options_copy = deepcopy(options[key])

    # 04.03.2024: check custom_threshold option
    if "custom_thresholds" in options_copy:
        if options_copy["custom_thresholds"]:
            raise RuntimeError("option 'custom_thresholds' is removed, port the code to using 'threshold_time_period_overrides'")
        logger.warn("option 'custom_thresholds' is not supported anymore, please remove it from configuration")

    # 04.03.2024: check 'threshold_time_period_overrides' option
    for account, value in options_copy.get("threshold_time_period_overrides", {}).items():
        if isinstance(value, Mapping):
            logger.warn(f"{key}/threshold_time_period_overrides/{account} should be list of dictionaries, not dictionary")
            options_copy[account] = [value]

    return options_copy


def run_check_impl(yt_client, logger, options, states):
    aggregated_state = states.FULLY_AVAILABLE_STATE
    aggregated_descriptions = []

    def combine_result(check_state, check_descriptions):
        nonlocal aggregated_state
        nonlocal aggregated_descriptions
        aggregated_state = min(aggregated_state, check_state)
        aggregated_descriptions += check_descriptions

    combine_result(*do_run_check(yt_client, logger, states, port_options(logger, options, "accounts"), AccountFacade, AccountConfig))
    combine_result(*do_run_check(yt_client, logger, states, port_options(logger, options, "bundles"), BundleFacade, BundleConfig))

    if aggregated_state == states.FULLY_AVAILABLE_STATE:
        return aggregated_state
    else:
        return aggregated_state, "; ".join(aggregated_descriptions)
