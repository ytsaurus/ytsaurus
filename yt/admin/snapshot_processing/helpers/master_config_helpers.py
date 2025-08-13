from .cypress_helpers import join_path

from yt.wrapper import yson


def patch_master_config(config, address):
    config["snapshots"] = {"path": "./snapshots"}
    config["changelogs"] = {"path": "./changelogs"}
    if "bus_server" in config:
        config["bus_server"].pop("port", None)
        config["bus_server"].pop("unix_domain_socket_path", None)
    fqdn = address.split(":")[0]
    config["address_resolver"]["localhost_fqdn"] = fqdn

    if "native_authentication_manager" in config:
        config["native_authentication_manager"]["enable_validation"] = False
        if "tvm_service" in config["native_authentication_manager"]:
            config["native_authentication_manager"].pop("tvm_service", None)

    return yson.dumps(config, yson_format="pretty")


def get_master_config(yt_client, config_path, address):
    return patch_master_config(yt_client.get(config_path), address)


def is_host_healthy(yt_client, host, masters_root_path):
    yt_client.config["proxy"]["retries"]["enable"] = False
    state = None
    try:
        state = yt_client.get(
            join_path(masters_root_path, host, "orchid", "monitoring", "hydra", "state"))
    except Exception:
        pass
    yt_client.config["proxy"]["retries"]["enable"] = True

    return state is not None and (state == "following" or state == "leading")


def find_healthy_host_address(yt_client, masters_root_path, host_list=None):
    # secondary masters of current cell_tag are already listed in host_list.
    host_list = host_list or yt_client.list(masters_root_path)

    for host in host_list:
        if is_host_healthy(yt_client, host, masters_root_path):
            return host
    raise RuntimeError("No host is either leading or following")

#####################################################################


def get_patched_primary(yt_client):
    primary_master_root = "//sys/primary_masters"
    host_address = find_healthy_host_address(yt_client, primary_master_root)
    return get_master_config(
        yt_client,
        join_path(primary_master_root, host_address, "orchid", "config"),
        host_address)


def get_patched_secondary(yt_client, specific_cell_tag=None):
    secondary_masters_root = "//sys/secondary_masters"
    host_list = yt_client.get(secondary_masters_root)

    if specific_cell_tag is not None and specific_cell_tag not in host_list.keys():
        return None

    config_strings = {}
    for cell_tag in host_list.keys():
        if specific_cell_tag is not None and cell_tag != specific_cell_tag:
            continue
        host_address = find_healthy_host_address(
            yt_client,
            join_path(secondary_masters_root, cell_tag),
            host_list[cell_tag].keys())
        config_string = get_master_config(
            yt_client,
            join_path(secondary_masters_root, cell_tag, host_address, "orchid", "config"),
            host_address)
        if specific_cell_tag is not None:
            return config_string
        config_strings[cell_tag] = config_string

    return config_strings
