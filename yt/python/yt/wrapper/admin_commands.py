from .driver import make_request


def switch_leader(cell_id=None, new_leader_address=None, client=None):
    """Switch leader of given master cell."""
    params = {
        "cell_id": cell_id,
        "new_leader_address": new_leader_address,
    }

    return make_request("switch_leader", params=params, client=client)


def add_switch_leader_parser(subparsers):
    parser = subparsers.add_parser("switch-leader", help="Switch master cell leader")
    parser.set_defaults(func=switch_leader)
    parser.add_argument("--cell-id")
    parser.add_argument("--new-leader-address", type=str)
