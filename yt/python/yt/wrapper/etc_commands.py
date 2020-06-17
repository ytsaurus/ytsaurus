from .driver import make_request, make_formatted_request
from .common import set_param
from .driver import get_api_version
from .batch_response import apply_function_to_result

# For backward compatibility.
from yt.ypath import parse_ypath


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
    :param resource_delta: the amount of transferred resources as a json-able object.
    """
    params = {
        "source_account": source_account,
        "destination_account": destination_account,
        "resource_delta": resource_delta
    }
    return make_request("transfer_account_resources", params=params, client=client)
