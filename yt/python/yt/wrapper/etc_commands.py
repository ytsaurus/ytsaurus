from .driver import make_request, make_formatted_request
from .common import set_param
from .driver import get_api_version
from .batch_response import apply_function_to_result

# For backward compatibility.
from yt.ypath import parse_ypath  # noqa

from typing import Optional


def execute_batch(requests, concurrency=None, client=None):
    """Executes `requests` in parallel as one batch request."""
    params = {
        "requests": requests
    }
    set_param(params, "concurrency", concurrency)
    return make_formatted_request("execute_batch", params=params, format=None, client=client)


def generate_timestamp(client=None):
    """Generates timestamp."""
    result = make_formatted_request("generate_timestamp", params={}, format=None, client=client)

    def _process_result(result):
        return result["timestamp"] if get_api_version(client) == "v4" else result

    return apply_function_to_result(_process_result, result)


def transfer_account_resources(source_account, destination_account, resource_delta, client=None):
    """Transfers resources between accounts.

    On the path from `source_account` to `destination_account` in the account tree, `resource_delta`
    is subtracted from `source_account` and its ancestors and added to `destination_account` and
    its ancestors. Limits of the lowest common ancestor remain unchanged.

    :param str source_account: account to transfer resources from.
    :param str destination_account: account to transfer resources to.
    :param resource_delta: the amount of transferred resources as a dict.
    """
    params = {
        "source_account": source_account,
        "destination_account": destination_account,
        "resource_delta": resource_delta
    }
    return make_request("transfer_account_resources", params=params, client=client)


def transfer_pool_resources(source_pool, destination_pool, pool_tree, resource_delta, client=None):
    """Transfers resources between pools.

    On the path from `source_pool` to `destination_pool` in the specified `pool_tree`, `resource_delta`
    is subtracted from `source_pool` and its ancestors and added to `destination_pool` and
    its ancestors. Limits of the lowest common ancestor remain unchanged.

    :param str source_pool: pool to transfer resources from.
    :param str destination_pool: pool to transfer resources to.
    :param resource_delta: the amount of transferred resources as a dict.
    """
    params = {
        "source_pool": source_pool,
        "destination_pool": destination_pool,
        "pool_tree": pool_tree,
        "resource_delta": resource_delta
    }
    return make_request("transfer_pool_resources", params=params, client=client)


def get_supported_features(format=None, client=None):
    """Retrieves supported cluster features (data types, codecs etc.)."""
    params = {}
    result = make_formatted_request("get_supported_features", params=params, format=format, client=client)
    return result["features"]


def check_cluster_liveness(check_cypress_root: bool = True, check_secondary_master_cells: bool = True,
                           check_tablet_cell_bundle: Optional[str] = None, client=None):
    """Checks cluster liveness. Raises an error if the cluster is not alive.

    :param bool check_cypress_root: check master.
    :param bool check_secondary_master_cells: check secondary masters.
    :param str check_tablet_cell_bundle: check that tablet cell bundle's health is good or degraded.
    """
    params = {}
    set_param(params, "check_cypress_root", check_cypress_root)
    set_param(params, "check_secondary_master_cells", check_secondary_master_cells)
    set_param(params, "check_tablet_cell_bundle", check_tablet_cell_bundle)
    make_request("check_cluster_liveness", params=params, client=client)
