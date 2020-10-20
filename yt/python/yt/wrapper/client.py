from .config import get_config, set_option
from .client_state import ClientState
from .client_helpers import create_class_method, initialize_client, are_signatures_equal
from . import client_api

import os
from copy import deepcopy


old_style_client = os.environ.get("YT_OLD_STYLE_CLIENT")

if not old_style_client:
    try:
        from .client_impl import YtClient

        for name in client_api.all_names:
            assert are_signatures_equal(getattr(YtClient, name), create_class_method(getattr(client_api, name)))
    except ImportError as err:
        old_style_client = True

if old_style_client:
    class YtClient(ClientState):
        """Implements YT client."""

        def __init__(self, proxy=None, token=None, config=None):
            super(YtClient, self).__init__()
            initialize_client(self, proxy, token, config)

    for name in client_api.all_names:
        setattr(YtClient, name, create_class_method(getattr(client_api, name)))


def create_client_with_command_params(client=None, **kwargs):
    """Creates new client with command params."""
    new_client = YtClient(config=deepcopy(get_config(client)))
    set_option("COMMAND_PARAMS", kwargs, new_client)
    return new_client

# Backward compatibility.
Yt = YtClient
