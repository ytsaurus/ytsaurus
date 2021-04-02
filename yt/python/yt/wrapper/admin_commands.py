from .common import YtError
from .cypress_commands import get, list
from .driver import make_request

def _find_cell_id_and_new_leader_id_by_master_address(specified_address, client):
    guessed_cell_id = None
    guessed_leader_id = None

    try:
        master_config = get("//sys/primary_masters/{}/orchid/config".format(specified_address), client=client)
    except YtError as err:
        if err.is_resolve_error() and specified_address not in list("//sys/primary_masters", client=client):
            raise YtError("Master {} is not found at //sys/primary_masters, "
                          "check that cluster proxy and master address speicified correctly".format(specified_address))
        else:
            raise
    for leader_id, address in enumerate(master_config["primary_master"]["addresses"]):
        if specified_address == address or specified_address == address.split(":")[0]:
            guessed_cell_id = master_config["primary_master"]["cell_id"]
            guessed_leader_id = leader_id
    for secondary_cell in master_config.get("secondary_masters", []):
        for leader_id, address in enumerate(secondary_cell["addresses"]):
            if specified_address == address or specified_address == address.split(":")[0]:
                guessed_cell_id = secondary_cell["cell_id"]
                guessed_leader_id = leader_id
    if "clock_servers" in master_config:
        for leader_id, address in enumerate(master_config["clock_servers"]["addresses"]):
            if specified_address == address or specified_address == address.split(":")[0]:
                guessed_cell_id = master_config["clock_servers"]["cell_id"]
                guessed_leader_id = leader_id

    if guessed_cell_id is None:
        raise YtError("Master address {} is not found in //sys/@cluster_connection".format(specified_address))
    else:
        return guessed_cell_id, guessed_leader_id

def switch_leader(cell_id=None, new_leader_id=None, new_master_address=None, client=None):
    """Switch leader of given master cell."""
    if new_master_address is not None:
        guessed_cell_id, guessed_new_leader_id = _find_cell_id_and_new_leader_id_by_master_address(
            new_master_address,
            client=client)
        if cell_id is not None and cell_id != guessed_cell_id:
            raise YtError("Specified cell_id {} does not match guessed cell_id {}".format(cell_id, guessed_cell_id))
        if new_leader_id is not None and new_leader_id != guessed_new_leader_id:
            raise YtError("Specified new_leader_id {} does not match peer_id of specified master {}"
                          .format(new_leader_id, guessed_new_leader_id))
        cell_id = guessed_cell_id
        new_leader_id = guessed_new_leader_id

    params = {
        "cell_id": cell_id,
        "new_leader_id": new_leader_id,
    }
    return make_request("switch_leader", params=params, client=client)

def add_switch_leader_parser(subparsers):
    parser = subparsers.add_parser("switch-leader", help="Switch master cell leader")
    parser.set_defaults(func=switch_leader)
    parser.add_argument("--cell-id")
    parser.add_argument("--new-leader-id", type=int)
    parser.add_argument("--new-master-address")
