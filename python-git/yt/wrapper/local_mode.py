from .cypress_commands import get
from .config import get_config
from .errors import YtResponseError

import socket

def is_local_mode(client):
    local_mode = get_config(client)["pickling"]["local_mode"]
    if local_mode is not None:
        return local_mode

    fqdn = None
    try:
        fqdn = get("//sys/@local_mode_fqdn", client=client)
    except YtResponseError as err:
        if not err.is_resolve_error():
            raise

    local_mode = fqdn == socket.getfqdn()
    get_config(client)["pickling"]["local_mode"] = local_mode
    return local_mode

