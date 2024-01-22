from yt_commands import lookup_rows, select_rows, get_driver, get

from yt.yson import YsonMap

from dataclasses import dataclass
from typing import Any, Dict, List


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


def lookup_rows_in_ground(*args, **kwargs):
    _use_ground_driver(kwargs)
    return lookup_rows(*args, **kwargs)


def select_rows_from_ground(*args, **kwargs):
    _use_ground_driver(kwargs)
    return select_rows(*args, **kwargs)


def _build_children_map_from_tables(id):
    rows = select_rows_from_ground(f"child_key, child_id from [{CHILD_NODE_TABLE.get_path()}] where parent_id = \"{id}\"")
    result = YsonMap()
    for row in rows:
        result[row["child_key"]] = row["child_id"]
    return result


def _extract_type_from_id(id):
    return int(id.split("-")[2], 16) & 0xffff


def validate_sequoia_tree_consistency(cluster="primary"):
    sequoia_node_rows = select_rows_from_ground(f"* from [{PATH_TO_NODE_ID_TABLE.get_path()}]", cluster=cluster)

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


@dataclass(frozen=True)
class SequoiaTable:
    name: str
    schema: List[Dict[str, Any]]

    def get_path(self):
        return "//sys/sequoia/" + self.name


PATH_TO_NODE_ID_TABLE = SequoiaTable(
    name="path_to_node_id",
    schema=[
        {"name": "path", "type": "string", "sort_order": "ascending"},
        {"name": "node_id", "type": "string"},
    ])

NODE_ID_TO_PATH_TABLE = SequoiaTable(
    name="node_id_to_path",
    schema=[
        {"name": "node_id", "type": "string", "sort_order": "ascending"},
        {"name": "path", "type": "string"},
    ])

CHILD_NODE_TABLE = SequoiaTable(
    name="child_node",
    schema=[
        {"name": "parent_id", "type": "string", "sort_order": "ascending"},
        {"name": "child_key", "type": "string", "sort_order": "ascending"},
        {"name": "child_id", "type": "string"},
    ])

CHUNK_REPLICAS_TABLE = SequoiaTable(
    name="chunk_replicas",
    schema=[
        {"name": "id_hash", "type": "uint32", "sort_order": "ascending"},
        {"name": "chunk_id", "type": "string", "sort_order": "ascending"},
        {"name": "replicas", "type": "any", "aggregate": "_yt_replica_set"},
    ])

LOCATION_REPLICAS_TABLE = SequoiaTable(
    name="location_replicas",
    schema=[
        {"name": "cell_tag", "type": "uint16", "sort_order": "ascending"},
        {"name": "node_id", "type": "uint32", "sort_order": "ascending"},
        {"name": "id_hash", "type": "uint32", "sort_order": "ascending"},
        {"name": "location_uuid", "type": "string", "sort_order": "ascending"},
        {"name": "chunk_id", "type": "string", "sort_order": "ascending"},
        {"name": "replica_index", "type": "int32"},
    ]
)

SEQUOIA_RESOLVE_TABLES = [
    PATH_TO_NODE_ID_TABLE,
    NODE_ID_TO_PATH_TABLE,
    CHILD_NODE_TABLE,
]

SEQUOIA_CHUNK_TABLES = [
    CHUNK_REPLICAS_TABLE,
    LOCATION_REPLICAS_TABLE,
]

SEQUOIA_TABLES = SEQUOIA_RESOLVE_TABLES + SEQUOIA_CHUNK_TABLES


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
        PATH_TO_NODE_ID_TABLE.get_path(),
        [{"path": mangle_sequoia_path(path)}])
    return rows[0]["node_id"] if rows else None


def resolve_sequoia_id(node_id):
    rows = lookup_rows_in_ground(
        NODE_ID_TO_PATH_TABLE.get_path(),
        [{"node_id": node_id}])
    return rows[0]["path"] if rows else None


def resolve_sequoia_children(node_id):
    return select_rows_from_ground(
        f"child_key, child_id from [{CHILD_NODE_TABLE.get_path()}] where node_id == \"{node_id}\"")
