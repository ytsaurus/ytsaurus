from .driver import make_request, make_formatted_request
from .common import set_param

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
    return make_formatted_request("generate_timestamp", params={}, format=None, client=client)
