from yt_commands import lookup_rows, select_rows, get_driver, get

from yt.yson import YsonMap

from yt.sequoia_tools import DESCRIPTORS


################################################################################

# EObjectType values.
SEQUOIA_MAP_NODE_OBJECT_ID = 1504
ROOTSTOCK_OBJECT_ID = 12000
SCION_OBJECT_ID = 12000


def get_ground_driver(cluster="primary"):
    return get_driver(cluster=cluster + "_ground")


def _use_ground_driver(kwargs):
    if "driver" not in kwargs:
        if "cluster" in kwargs:
            kwargs["driver"] = get_ground_driver(kwargs["cluster"])
        else:
            kwargs["driver"] = get_ground_driver()


# Select from a sorted table is not guaranteed to be sorted if limit is not specified.
# It's a good idea to always add some sort of limit unless user specifically requested another value, or None (to remove limits).
def _add_limit_to_request(query, kwargs):
    if "limit" in query:
        return query

    limit = kwargs.get("limit", 100000)
    if limit is None:
        return query

    query += f" limit {limit}"
    return query


def lookup_rows_in_ground(path, queries, **kwargs):
    _use_ground_driver(kwargs)
    return lookup_rows(path, queries, **kwargs)


def select_rows_from_ground(query, **kwargs):
    _use_ground_driver(kwargs)
    updated_query = _add_limit_to_request(query, kwargs)
    return select_rows(updated_query, **kwargs)


def _build_children_map_from_tables(id):
    rows = select_rows_from_ground(f"child_key, child_id from [{DESCRIPTORS.child_node.get_default_path()}] where parent_id = \"{id}\"")
    result = YsonMap()
    for row in rows:
        result[row["child_key"]] = row["child_id"]
    return result


def _extract_type_from_id(id):
    return int(id.split("-")[2], 16) & 0xffff


def validate_sequoia_tree_consistency(cluster="primary"):
    sequoia_node_rows = select_rows_from_ground(f"* from [{DESCRIPTORS.path_to_node_id.get_default_path()}]", cluster=cluster)

    supported_types = [SEQUOIA_MAP_NODE_OBJECT_ID, ROOTSTOCK_OBJECT_ID, SCION_OBJECT_ID]
    for row in sequoia_node_rows:
        if _extract_type_from_id(row["node_id"]) not in supported_types:
            continue

        # Alternative way of looking things up can be written using a combination of ls and get commands.
        # Might want to check consistency using both methods.
        # TODO(h0pless): Think about it.
        unmangled_path = row["path"][:-1]
        children_master = get(f"{unmangled_path}/@children")
        children_tables = _build_children_map_from_tables(row["node_id"])

        if children_tables != children_master:
            tables_info_set = set(children_tables.items())
            master_info_set = set(children_master.items())
            raise RuntimeError(
                f"Sequoia tree is not consistent between master and dynamic tables for node: {unmangled_path}\n"
                f"Master entries missing in dynamic table: {master_info_set - tables_info_set}\n"
                f"Dynamic table entries missing in master: {tables_info_set - master_info_set}")


################################################################################


def get_sequoia_table_path(name):
    return f"//sys/sequoia/{name}"


def mangle_sequoia_path(path):
    assert not path.endswith('/')
    return path + '/'


def demangle_sequoia_path(path):
    assert path.endswith('/')
    return path[:-1]


def resolve_sequoia_path(path):
    rows = lookup_rows_in_ground(
        DESCRIPTORS.path_to_node_id.get_default_path(),
        [{"path": mangle_sequoia_path(path)}])
    return rows[0]["node_id"] if rows else None


def resolve_sequoia_id(node_id):
    rows = lookup_rows_in_ground(
        DESCRIPTORS.node_id_to_path.get_default_path(),
        [{"node_id": node_id}])
    return rows[0]["path"] if rows else None


def resolve_sequoia_children(node_id):
    return select_rows_from_ground(
        f"child_key, child_id from [{DESCRIPTORS.child_node.get_default_path()}] where node_id == \"{node_id}\"")
