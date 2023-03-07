from .driver import make_formatted_request
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
