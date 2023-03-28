from .batch_response import apply_function_to_result
from .common import set_param
from .driver import get_api_version, make_request, make_formatted_request
from .errors import YtError


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


def add_maintenance(component, address, type, comment, client=None):
    """Adds maintenance request for given node

    :param component: component type. There are 4 component types: `cluster_node`, `http_proxy`, `rpc_proxy`, `host`.
    :param address: component address.
    :param type: maintenance type. There are 5 maintenance types: ban, decommission, disable_scheduler_jobs,
    disable_write_sessions, disable_tablet_cells.
    :param comment: any string with length not larger than 512 characters.
    :return: unique (per component) maintenance id.
    """
    params = {
        "component": component,
        "address": address,
        "type": type,
        "comment": comment
    }

    request = make_formatted_request("add_maintenance", params=params, format=None, client=client)

    def _process_result(result):
        try:
            return result["id"] if get_api_version(client) == "v4" else result
        except TypeError:
            return result

    return apply_function_to_result(_process_result, request)


def remove_maintenance(component,
                       address,
                       id=None,
                       ids=None,
                       type=None,
                       user=None,
                       mine=False,
                       all=False,
                       client=None):
    """Removes maintenance requests from given node by id or filter.

    :param component: component type. There are 4 component types: `cluster_node`, `http_proxy`, `rpc_proxy`, `host`.
    :param address: component address.
    :param ids: maintenance ids. Only maintenance requests which id is listed can be removed.
    :param id: single maintenance id. The same as `ids` but accepts single id instead of list.
    Cannot be used at the same time with `ids`.
    :param type: maintenance type. If set only maintenance requests with given type will be removed.
    There are 5 maintenance types: ban, decommission, disable_scheduler_jobs, disable_write_sessions,
    disable_tablet_cells.
    :param user: only maintenance requests with this user will be removed.
    :param mine: only maintenance requests with authenticated user will be removed.
    Cannot be used with `user`.
    :param all: all maintenance requests from given node will be removed.
    Cannot be used with other options.
    :return: Dictionary with removed maintenance request count for each maintenance type.
    """
    params = {
        "component": component,
        "address": address,
    }

    if not (user or mine or all or type or id or ids):
        raise YtError("At least one of {\"id\", \"all\", \"type\", \"user\", \"mine\"} must be specified")

    if id and ids:
        raise YtError("At most one of {\"id\", \"ids\"} can be specified")

    if id is not None:
        set_param(params, "id", id)
    elif ids is not None:
        set_param(params, "ids", ids)
    if type is not None:
        set_param(params, "type", type)
    if mine:
        set_param(params, "mine", True)
    if all:
        set_param(params, "all", True)
    if user is not None:
        set_param(params, "user", user)

    return make_formatted_request("remove_maintenance", params=params, format=None, client=client)


def disable_chunk_locations(node_address, location_uuids, client=None):
    """Disable locations by uuids.

    :param node_address: node address.
    :param location_uuids: location uuids.
    """
    params = {
        "node_address": node_address,
        "location_uuids": location_uuids
    }

    return make_request("disable_chunk_locations", params=params, client=client)


def destroy_chunk_locations(node_address, location_uuids, client=None):
    """Mark locations for destroing. Disks of these locations can be recovered.

    :param node_address: node address.
    :param location_uuids: location uuids.
    """
    params = {
        "node_address": node_address,
        "location_uuids": location_uuids
    }

    return make_request("destroy_chunk_locations", params=params, client=client)


def resurrect_chunk_locations(node_address, location_uuids, client=None):
    """Try resurrect disabled locations.

    :param node_address: node address.
    :param location_uuids: location uuids.
    """
    params = {
        "node_address": node_address,
        "location_uuids": location_uuids
    }

    return make_request("resurrect_chunk_locations", params=params, client=client)
