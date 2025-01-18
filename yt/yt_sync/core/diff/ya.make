PY3_LIBRARY()

STYLE_PYTHON()


PY_SRCS(
    __init__.py
    base.py
    db_diff.py
    node_attributes_change.py
    missing_node.py
    missing_table.py
    orphaned_table.py
    replicas_change.py
    schema_change.py
    table_attributes_change.py
    tablet_count_change.py
)

PEERDIR(
    yt/yt_sync/core/helpers
    yt/yt_sync/core/model
    yt/yt_sync/core/settings
)

END()

RECURSE_FOR_TESTS(
    ut
)
