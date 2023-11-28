from yt_commands import lookup_rows, select_rows, get_driver

from dataclasses import dataclass
from typing import Any, Dict, List


################################################################################


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


################################################################################


@dataclass(frozen=True)
class SequoiaTable:
    name: str
    schema: List[Dict[str, Any]]

    def get_path(self):
        return "//sys/sequoia/" + self.name


RESOLVE_NODE = SequoiaTable(
    name="resolve_node",
    schema=[
        {"name": "path", "type": "string", "sort_order": "ascending"},
        {"name": "node_id", "type": "string"},
    ])

REVERSE_RESOLVE_NODE = SequoiaTable(
    name="reverse_resolve_node",
    schema=[
        {"name": "node_id", "type": "string", "sort_order": "ascending"},
        {"name": "path", "type": "string"},
    ])

CHILDREN_NODES = SequoiaTable(
    name="children_nodes",
    schema=[
        {"name": "parent_path", "type": "string", "sort_order": "ascending"},
        {"name": "child_key", "type": "string", "sort_order": "ascending"},
        {"name": "child_id", "type": "string"},
    ])

CHUNK_REPLICAS = SequoiaTable(
    name="chunk_replicas",
    schema=[
        {"name": "id_hash", "type": "uint32", "sort_order": "ascending"},
        {"name": "chunk_id", "type": "string", "sort_order": "ascending"},
        {"name": "location_uuid", "type": "string", "sort_order": "ascending"},
        {"name": "replica_index", "type": "int32", "sort_order": "ascending"},
        {"name": "node_id", "type": "uint32"},
    ])

LOCATION_REPLICAS = SequoiaTable(
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
    RESOLVE_NODE,
    REVERSE_RESOLVE_NODE,
    CHILDREN_NODES,
]

SEQUOIA_CHUNK_TABLES = [
    CHUNK_REPLICAS,
    LOCATION_REPLICAS,
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
        RESOLVE_NODE.get_path(),
        [{"path": mangle_sequoia_path(path)}])
    return rows[0]["node_id"] if rows else None


def resolve_sequoia_id(node_id):
    rows = lookup_rows_in_ground(
        REVERSE_RESOLVE_NODE.get_path(),
        [{"node_id": node_id}])
    return rows[0]["path"] if rows else None


def resolve_sequoia_children(node_id):
    return select_rows_from_ground(
        f"child_key, child_id from [{CHILDREN_NODES.get_path()}] where node_id == \"{node_id}\"")
