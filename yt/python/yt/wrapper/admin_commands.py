from .driver import make_request


def build_snapshot(cell_id=None, client=None):
    """Builds snapshot of a given cell."""
    params = {
        "cell_id": cell_id,
    }

    return make_request("build_snapshot", params=params, client=client)


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


def suspend_tablet_cells(cell_ids, client=None):
    """Suspends listed tablet cells."""
    params = {
        "cell_ids": cell_ids,
    }

    return make_request("suspend_tablet_cells", params=params, client=client)


def resume_tablet_cells(cell_ids, client=None):
    """Resumes listed tablet cells."""
    params = {
        "cell_ids": cell_ids,
    }

    return make_request("resume_tablet_cells", params=params, client=client)
