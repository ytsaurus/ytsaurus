from .config import get_config, set_option
from .client_helpers import create_class_method, are_signatures_equal
from . import client_api

from copy import deepcopy


try:
    from .client_impl import YtClient

    for name in client_api.all_names:
        assert are_signatures_equal(getattr(YtClient, name), create_class_method(getattr(client_api, name)))
except AssertionError:
    YtClient = None


def create_client_with_command_params(client=None, **kwargs):
    """Creates new client with command params."""
    new_client = YtClient(config=deepcopy(get_config(client)))
    set_option("COMMAND_PARAMS", kwargs, new_client)
    return new_client

# Backward compatibility.
Yt = YtClient
