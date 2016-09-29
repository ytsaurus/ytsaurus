from .cypress_commands import get
from .config import get_option, set_option
from .errors import YtResponseError

import socket

def is_local_mode(client):
    if get_option("_is_local_mode", client) is not None:
        return get_option("_is_local_mode", client)

    fqdn = None
    try:
        fqdn = get("//sys/@local_mode_fqdn", client=client)
    except YtResponseError as err:
        if not err.is_resolve_error():
            raise

    is_local_mode = (fqdn is not None) and fqdn == socket.getfqdn()

    set_option("_is_local_mode", is_local_mode, client)

    return is_local_mode

