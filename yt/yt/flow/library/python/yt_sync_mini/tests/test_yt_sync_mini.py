"""Schema-drift regression test for ``yt_sync_mini``.

The embedded ``definitions.yson`` resource is shared by ``yt_sync_mini``,
``yt_sync``, and the native client.

This test creates a pipeline via ``client.create("pipeline", ...)``
(i.e. through ``NYT::NFlow::CreatePipelineNode``) and asserts that the Python
catalogue and cpp agree on:

* The set of inner table paths, recursively below intermediate directories.
  The known prebuilt ytserver resource 13574877676 lacks the log tables;
  only that resource is exempt from checking their presence.
* The column schema of each table, including canonical logical types.
* Schema strictness and the unique-key flag.

The test does NOT compare richer per-column attributes (``group``,
``max_inline_hunk_size``), since the recipe uses a prebuilt ytserver that may
lag behind the working-copy resource. Hermetic loader/consumer tests cover
those attributes.
"""

import os
import re

import pytest
import yatest.common

import yt.wrapper as yt

from yt.yt.flow.library.python.pipeline_tables.schemas import (
    PIPELINE_QUEUES,
    PIPELINE_TABLES,
)

# Per-column attributes that cpp sets and the Python schema entry must match.
# ``group`` and ``max_inline_hunk_size`` are covered by hermetic resource tests;
# the recipe's prebuilt ytserver may lag behind the working copy.
COMPARED_COLUMN_KEYS = ("name", "sort_order", "expression")

LOG_TABLE_PATHS = {
    "logs/logs",
    "logs/logs_meta",
    "logs/sources_meta",
}
TABLE_NODE_TYPES = {"table", "replicated_table", "chaos_replicated_table"}
PREBUILT_YTSERVER_WITHOUT_LOG_TABLES = "13574877676"


def _tables_missing_from_prebuilt_ytserver(definition, server_binary):
    if not os.path.isfile(server_binary) or not os.path.isfile(definition):
        return set()
    with open(definition) as source:
        resource = re.search(r"FROM_SANDBOX\(\s*(?:FILE\s+)?(\d+)\b", source.read())
    if resource and resource[1] == PREBUILT_YTSERVER_WITHOUT_LOG_TABLES:
        return LOG_TABLE_PATHS
    return set()


@pytest.mark.parametrize(
    "resource,definition_exists,binary_exists,expected",
    [
        (PREBUILT_YTSERVER_WITHOUT_LOG_TABLES, True, True, LOG_TABLE_PATHS),
        ("13970901195", True, True, set()),
        (PREBUILT_YTSERVER_WITHOUT_LOG_TABLES, True, False, set()),
        (PREBUILT_YTSERVER_WITHOUT_LOG_TABLES, False, True, set()),
    ],
    ids=["known_prebuilt", "updated_resource", "source_build", "no_internal_package"],
)
def test_prebuilt_log_table_exemption(tmp_path, resource, definition_exists, binary_exists, expected):
    definition = tmp_path / "ya.make"
    server_binary = tmp_path / "ytserver-all"
    if definition_exists:
        definition.write_text(f"FROM_SANDBOX(\n    {resource}\n)\n")
    if binary_exists:
        server_binary.touch()
    assert _tables_missing_from_prebuilt_ytserver(definition, server_binary) == expected


@pytest.fixture(scope="module")
def yt_client():
    proxy = os.environ.get("YT_PROXY_PRIMARY") or os.environ.get("YT_PROXY")
    assert proxy, "YT_PROXY[_PRIMARY] env var must be set by the YT recipe"
    return yt.YtClient(proxy=proxy)


def _normalise_column(column):
    """Project a column dict to the keys we compare across cpp/Python."""
    result = {key: column.get(key) for key in COMPARED_COLUMN_KEYS if column.get(key) is not None}
    type_v3 = column.get("type_v3")
    if type_v3 is None:
        legacy_type = column["type"]
        type_v3 = {
            "type_name": "optional",
            "item": {"any": "yson", "boolean": "bool"}.get(legacy_type, legacy_type),
        }
    result["type_v3"] = type_v3
    return result


def test_python_catalogue_matches_cpp(yt_client):
    """Python catalogue and native ``GetPipelineTableDefinitions()`` must agree."""
    server_path = "//tmp/yt_sync_mini_drift"
    if yt_client.exists(server_path):
        yt_client.remove(server_path, recursive=True, force=True)
    yt_client.create("pipeline", server_path, recursive=True)

    errors = []
    cpp_table_paths = set()

    def collect_tables(directory, prefix=""):
        for raw_child in yt_client.list(directory):
            child = str(raw_child)
            if "/" in child:
                errors.append(f"cpp CreatePipelineNode created a literal slash-containing child {child!r}.")
                continue
            relative_path = f"{prefix}{child}"
            child_path = f"{directory}/{child}"
            node_type = yt_client.get(f"{child_path}/@type")
            if node_type == "map_node":
                collect_tables(child_path, f"{relative_path}/")
            elif node_type in TABLE_NODE_TYPES:
                cpp_table_paths.add(relative_path)
            else:
                errors.append(f"cpp CreatePipelineNode created unexpected {relative_path!r} of type {node_type!r}.")

    collect_tables(server_path)

    python_entries = dict(PIPELINE_TABLES)
    python_entries.update(PIPELINE_QUEUES)
    python_known = set(python_entries)

    missing_in_python = sorted(cpp_table_paths - python_known)
    if missing_in_python:
        errors.append(
            f"cpp CreatePipelineNode creates {missing_in_python} but the Python catalogue "
            "(yt/yt/flow/library/python/pipeline_tables/schemas.py) does not."
        )

    package = "yt/packages/ytflow_latest"
    allowed_missing = _tables_missing_from_prebuilt_ytserver(
        yatest.common.source_path(f"{package}/ya.make"),
        yatest.common.build_path(f"{package}/ytserver-all"),
    )
    missing_in_cpp = sorted(python_known - cpp_table_paths - allowed_missing)
    if missing_in_cpp:
        errors.append(
            f"Python catalogue lists {missing_in_cpp} but a pipeline node does not get them. "
            "The node is materialized by the native client compiled into the recipe's prebuilt "
            "ytserver, so a table just added to the shared definitions.yson shows up here only after "
            "yt/packages/ytflow_latest is rebuilt and its resource pin bumped."
        )

    for table_path in sorted(cpp_table_paths & python_known):
        entry = python_entries[table_path]
        python_columns = [_normalise_column(column) for column in entry["schema"]]
        server_schema = yt_client.get(f"{server_path}/{table_path}/@schema")
        server_columns = [_normalise_column(dict(column)) for column in server_schema]
        if python_columns != server_columns:
            errors.append(
                f"table {table_path!r}: column schema drifted from cpp.\n"
                f"  server: {server_columns}\n"
                f"  python: {python_columns}"
            )
        expected_unique_keys = any(column.get("sort_order") for column in entry["schema"])
        if not bool(server_schema.attributes.get("strict")):
            errors.append(f"table {table_path!r}: cpp schema is not strict")
        if bool(server_schema.attributes.get("unique_keys")) != expected_unique_keys:
            errors.append(
                f"table {table_path!r}: cpp unique_keys={server_schema.attributes.get('unique_keys')!r}, "
                f"expected {expected_unique_keys!r}"
            )
        if table_path in LOG_TABLE_PATHS:
            max_data_ttl = 86_400_000 if table_path == "logs/logs" else 172_800_000
            for name, expected in (("min_data_versions", 0), ("min_data_ttl", 0), ("max_data_ttl", max_data_ttl)):
                actual = yt_client.get(f"{server_path}/{table_path}/@mount_config/{name}")
                if actual != expected:
                    errors.append(f"table {table_path!r}: cpp {name}={actual!r}, expected {expected}")

    if errors:
        pytest.fail("\n".join(errors))
