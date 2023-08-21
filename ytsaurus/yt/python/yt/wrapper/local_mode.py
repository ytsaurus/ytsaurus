from .cypress_commands import get
from .config import get_config, get_option, set_option
from .errors import YtResponseError
from .http_helpers import get_fqdn


def get_local_mode_fqdn(client):
    if get_option("_local_mode_fqdn", client) is not None:
        return get_option("_local_mode_fqdn", client)

    fqdn = None
    try:
        fqdn = get("//sys/@local_mode_fqdn", read_from="cache", client=client)
    except YtResponseError as err:
        if not err.is_resolve_error():
            raise

    set_option("_local_mode_fqdn", fqdn, client)

    return fqdn


def get_local_mode_proxy_address(client):
    if get_option("_local_mode_proxy_address", client) is not None:
        return get_option("_local_mode_proxy_address", client)

    address = None
    try:
        address = get("//sys/@local_mode_proxy_address", read_from="cache", client=client)
    except YtResponseError as err:
        if not err.is_resolve_error():
            raise

    set_option("_local_mode_proxy_address", address, client)

    return address


def is_local_mode(client):
    if get_config(client)["is_local_mode"] is not None:
        return get_config(client)["is_local_mode"]

    return get_local_mode_fqdn(client) is not None


def enable_local_files_usage_in_job(client):
    if get_config(client)["pickling"]["enable_local_files_usage_in_job"] is not None:
        return get_config(client)["pickling"]["enable_local_files_usage_in_job"]

    return is_local_mode(client) and get_local_mode_fqdn(client) == get_fqdn(client)
