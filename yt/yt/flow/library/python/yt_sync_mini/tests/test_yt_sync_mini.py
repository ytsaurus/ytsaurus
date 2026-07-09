"""Schema-drift regression test for ``yt_sync_mini``.

The Python-side schema catalogue
(``yt/yt/flow/library/python/pipeline_tables/schemas.py``) is the
single source of truth shared between ``yt_sync_mini`` and ``yt_sync``.
The cpp counterpart is ``yt/yt/flow/lib/native_client/pipeline_init.cpp:GetTables()``;
both must stay in sync.

This test creates a pipeline via ``client.create("pipeline", ...)``
(i.e. through ``NYT::NFlow::CreatePipelineNode``) and asserts that the Python
catalogue and cpp agree on:

* The set of inner tables (both directions: missing-in-Python and
  missing-in-cpp are bugs).
* The column schema of each table (compared by ``name``, ``type``,
  ``sort_order``, ``expression``).

The test does NOT compare richer per-column attributes (``group``,
``max_inline_hunk_size``) — those are yt_sync-only schema enrichments not
present in cpp ``GetTables()``.
"""

import os

import pytest

import yt.wrapper as yt

from yt.yt.flow.library.python.pipeline_tables.schemas import (
    PIPELINE_QUEUES,
    PIPELINE_TABLES,
)

# Per-column attributes that cpp sets and the Python schema entry must match.
# ``group`` and ``max_inline_hunk_size`` are deliberately excluded — they're
# yt_sync-only schema enrichments not present in cpp ``GetTables()``.
COMPARED_COLUMN_KEYS = ("name", "type", "sort_order", "expression")


@pytest.fixture(scope="module")
def yt_client():
    proxy = os.environ.get("YT_PROXY_PRIMARY") or os.environ.get("YT_PROXY")
    assert proxy, "YT_PROXY[_PRIMARY] env var must be set by the YT recipe"
    return yt.YtClient(proxy=proxy)


def _normalise_column(column):
    """Project a column dict to the keys we compare across cpp/Python."""
    return {key: column.get(key) for key in COMPARED_COLUMN_KEYS if column.get(key) is not None}


def test_python_catalogue_matches_cpp(yt_client):
    """Python catalogue and cpp ``GetTables()`` must agree on tables and schemas."""
    server_path = "//tmp/yt_sync_mini_drift"
    if yt_client.exists(server_path):
        yt_client.remove(server_path, recursive=True, force=True)
    yt_client.create("pipeline", server_path, recursive=True)

    cpp_children = set(yt_client.list(server_path))
    python_known = set(PIPELINE_TABLES) | set(PIPELINE_QUEUES)

    errors = []

    missing_in_python = sorted(cpp_children - python_known)
    if missing_in_python:
        errors.append(
            f"cpp CreatePipelineNode creates {missing_in_python} but the Python catalogue "
            "(yt/yt/flow/library/python/pipeline_tables/schemas.py) does not."
        )

    missing_in_cpp = sorted(python_known - cpp_children)
    if missing_in_cpp:
        errors.append(
            f"Python catalogue lists {missing_in_cpp} but cpp ``GetTables()`` "
            "(yt/yt/flow/lib/native_client/pipeline_init.cpp) does not create them."
        )

    for table_name in sorted(cpp_children & python_known):
        entry = PIPELINE_TABLES.get(table_name) or PIPELINE_QUEUES[table_name]
        python_columns = [_normalise_column(column) for column in entry["schema"]]
        server_columns = [
            _normalise_column(dict(column)) for column in yt_client.get(f"{server_path}/{table_name}/@schema")
        ]
        if python_columns != server_columns:
            errors.append(
                f"table {table_name!r}: column schema drifted from cpp.\n"
                f"  server: {server_columns}\n"
                f"  python: {python_columns}"
            )

    if errors:
        pytest.fail("\n".join(errors))
