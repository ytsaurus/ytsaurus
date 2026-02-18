from yt_commands import (
    get_driver, get,
    lookup_rows, select_rows, delete_rows, insert_rows)

from yt.yson import YsonMap
from yt.yson.yson_token import TOKEN_SLASH, TOKEN_LITERAL, TOKEN_END_OF_STREAM

from yt.sequoia_tools import DESCRIPTORS, TableDescriptor

from yt.wrapper.ypath import escape_ypath_literal, unescape_ypath_literal
from yt.ypath import YPathTokenizer

import decorator
import pytest


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


def delete_rows_from_ground(descriptor: TableDescriptor, rows, **kwargs):
    _use_ground_driver(kwargs)
    delete_rows(descriptor.get_default_path(), rows, **kwargs)


def insert_rows_to_ground(descriptor: TableDescriptor, rows, **kwargs):
    _use_ground_driver(kwargs)
    insert_rows(descriptor.get_default_path(), rows, **kwargs)


def clear_table_in_ground(descriptor: TableDescriptor, **kwargs):
    _use_ground_driver(kwargs)
    keys = [c["name"] for c in descriptor.schema if "sort_order" in c and "expression" not in c]
    rows = select_rows_from_ground(", ".join(keys) + f" from [{descriptor.get_default_path()}]")
    delete_rows(descriptor.get_default_path(), rows, **kwargs)


################################################################################


def _build_children_map_from_tables(id):
    rows = select_rows_from_ground(f"child_key, child_id from [{DESCRIPTORS.child_nodes.get_default_path()}] where parent_id = \"{id}\"")
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
        demangled_path = demangle_sequoia_path(row["path"])
        children_master = get(f"{demangled_path}/@children")
        children_tables = _build_children_map_from_tables(row["node_id"])

        if children_tables != children_master:
            tables_info_set = set(children_tables.items())
            master_info_set = set(children_master.items())
            raise RuntimeError(
                f"Sequoia tree is not consistent between master and dynamic tables for node: {demangled_path}\n"
                f"Master entries missing in dynamic table: {master_info_set - tables_info_set}\n"
                f"Dynamic table entries missing in master: {tables_info_set - master_info_set}")


################################################################################


def mangle_sequoia_path(path: str) -> str:
    tokenizer = YPathTokenizer(path.encode("utf-8"))
    tokenizer.advance()

    assert tokenizer.get_type() == TOKEN_SLASH
    mangled_path: bytearray = tokenizer.get_token()

    tokenizer.advance()
    while tokenizer.get_type() != TOKEN_END_OF_STREAM:
        assert tokenizer.skip(TOKEN_SLASH)
        assert tokenizer.get_type() == TOKEN_LITERAL

        mangled_path += b"\0"
        mangled_path += unescape_ypath_literal(tokenizer.get_token())

        tokenizer.advance()

    mangled_path += b"\0"

    return mangled_path.decode("utf-8")


def demangle_sequoia_path(path: str) -> str:
    assert path.startswith("/")
    assert path.endswith("\0")

    return "/".join(
        escape_ypath_literal(x)
        for x in path.split("\0")[:-1])[1:]


def select_paths_from_ground(*, fetch_sys_dir=False, **kwargs):
    query = f"path from [{DESCRIPTORS.path_to_node_id.get_default_path()}]"
    if not fetch_sys_dir:
        query += f' where not is_prefix("{mangle_sequoia_path("//sys")}", path)'
    return [
        demangle_sequoia_path(row["path"])
        for row in select_rows_from_ground(query, **kwargs)
    ]


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
        f"child_key, child_id from [{DESCRIPTORS.child_nodes.get_default_path()}] where node_id == \"{node_id}\"")


def lookup_cypress_transaction(transaction_id):
    result = lookup_rows_in_ground(DESCRIPTORS.transactions.get_default_path(), [{"transaction_id": transaction_id}])
    return result[0] if result else None


def select_cypress_transaction_replicas(transaction_id):
    return sorted([
        record["cell_tag"] for record in
        select_rows_from_ground(
            f"cell_tag from [{DESCRIPTORS.transaction_replicas.get_default_path()}] "
            f"where transaction_id = \"{transaction_id}\"")
    ])


def select_cypress_transaction_descendants(ancestor_id):
    return sorted([
        record["descendant_id"] for record in
        select_rows_from_ground(
            f"descendant_id from [{DESCRIPTORS.transaction_descendants.get_default_path()}] "
            f"where transaction_id = \"{ancestor_id}\"")
    ])


def select_cypress_transaction_prerequisites(transaction_id):
    return sorted([
        record["transaction_id"] for record in
        select_rows_from_ground(
            f"transaction_id from [{DESCRIPTORS.dependent_transactions.get_default_path()}] "
            f"where dependent_transaction_id = \"{transaction_id}\"")
    ])


################################################################################


def not_implemented_in_sequoia(func):
    def wrapper(func, self, *args, **kwargs):
        for cluster_index in range(self.NUM_REMOTE_CLUSTERS + 1):
            if self.get_param("ENABLE_TMP_ROOTSTOCK", cluster_index) or \
                    self.get_param("ENABLE_SYS_OPERATIONS_ROOTSTOCK", cluster_index):
                pytest.skip("Not implemented in Sequoia")
        return func(self, *args, **kwargs)

    return decorator.decorate(func, wrapper)


def not_implemented_for_mirrored_tx(func):
    def wrapper(func, self, *args, **kwargs):
        if self.ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA:
            pytest.skip("Not implemented for mirrored transactions")
        return func(self, *args, **kwargs)

    return decorator.decorate(func, wrapper)


def cannot_be_implemented_in_sequoia(reason):
    def wrapper_factory(func):
        def wrapper(func, self, *args, **kwargs):
            if self.ENABLE_TMP_ROOTSTOCK or self.ENABLE_SYS_OPERATIONS_ROOTSTOCK:
                pytest.skip(f"Cannot be implemented in Sequoia: {reason}")
            return func(self, *args, **kwargs)

        return decorator.decorate(func, wrapper)

    return wrapper_factory


##################################################################


def _is_well_known_id(object_id):
    return (int(object_id.split("-")[0], 16) & 0x80000000) != 0


def is_sequoia_id(object_id):
    if _is_well_known_id(object_id):
        return False

    # Check Sequoia bit.
    return (int(object_id.split("-")[0], 16) & 0x40000000) != 0


def is_cypress_tx_id(object_id):
    # Either transaction or nested_transaction.
    return (int(object_id.split("-")[2], 16) & 0xffff) in (1, 4)
