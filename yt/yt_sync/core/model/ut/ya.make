PY3TEST()

STYLE_PYTHON()

TEST_SRCS(
    helpers.py
    test_cluster.py
    test_column.py
    test_database.py
    test_helpers.py
    test_node_attributes.py
    test_node.py
    test_replica.py
    test_schema.py
    test_table_attributes.py
    test_table.py
    test_tablet_info.py
    test_tablet_state.py
)

PEERDIR(
    yt/python/client
    yt/yt_sync/core/constants
    yt/yt_sync/core/fixtures
    yt/yt_sync/core/model
    yt/yt_sync/core/test_lib
)

END()
