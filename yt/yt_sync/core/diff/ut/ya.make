PY3TEST()

STYLE_PYTHON()

TEST_SRCS(
    conftest.py
    helpers.py
    test_base.py
    test_db_diff.py
    test_missing_node.py
    test_missing_table.py
    test_node_attributes_change.py
    test_orphaned_table.py
    test_replicas_change.py
    test_schema_change.py
    test_table_attributes_change.py
    test_tablet_count_change.py
)

PEERDIR(
    yt/yt_sync/core/diff
    yt/yt_sync/core/fixtures
    yt/yt_sync/core/helpers
    yt/yt_sync/core/model
    yt/yt_sync/core/settings
)

END()
