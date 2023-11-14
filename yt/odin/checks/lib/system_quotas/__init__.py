from datetime import datetime
from collections import defaultdict


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


def get_not_so_critical_quota_holders(cluster, holders_on_all_clusters, holders_by_cluster):
    return holders_on_all_clusters | holders_by_cluster.get(cluster, set())


def build_link_to_account(cluster, account):
    return f"https://yt.yandex-team.ru/{cluster}/accounts/general?account={account}"


def build_link_to_bundle(cluster, bundle):
    return f"https://yt.yandex-team.ru/{cluster}/tablet_cell_bundles/tablet_cells?activeBundle={bundle}"


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
