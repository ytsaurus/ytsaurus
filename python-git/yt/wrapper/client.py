from .config import get_config, set_option
from .client_state import ClientState
from .client_helpers import create_class_method, initialize_client
from . import client_api


from copy import deepcopy


class YtClient(ClientState):
    """Implements YT client."""

    def __init__(self, proxy=None, token=None, config=None):
        super(YtClient, self).__init__()
        initialize_client(self, proxy, token, config)

def create_client_with_command_params(client=None, **kwargs):
    """Creates new client with command params."""
    new_client = YtClient(config=deepcopy(get_config(client)))
    set_option("COMMAND_PARAMS", kwargs, new_client)
    return new_client

for name in client_api.all_names:
    setattr(YtClient, name, create_class_method(getattr(client_api, name)))

# Backward compatibility.
Yt = YtClient
