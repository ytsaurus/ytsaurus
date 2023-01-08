from .batch_response import apply_function_to_result
from .driver import make_request

import json


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


def add_maintenance(node_address, maintenance_type, comment, client=None):
    """Add maintenance request for given node"""
    params = {
        "node_address": node_address,
        "type": maintenance_type,
        "comment": comment
    }

    def _process_result(result):
        try:
            return str(json.loads(result)["id"])
        except Exception:
            return result

    result = make_request("add_maintenance", params=params, client=client)
    return apply_function_to_result(_process_result, result)


def remove_maintenance(node_address, maintenance_id, client=None):
    """Remove maintenance request from given node"""
    params = {
        "node_address": node_address,
        "id": maintenance_id,
    }

    return make_request("remove_maintenance", params=params, client=client)


def release_locations(node_address, location_guids, client=None):
    """Mark locations for decommissioning"""
    params = {
        "node_address": node_address,
        "location_guids": location_guids
    }

    return make_request("release_locations", params=params, client=client)
