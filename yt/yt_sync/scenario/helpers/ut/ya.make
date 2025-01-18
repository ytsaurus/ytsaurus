PY3TEST()

STYLE_PYTHON()

TEST_SRCS(
    test_node_manager.py
    test_switcher.py
)

PEERDIR(
    yt/yt_sync/core
    yt/yt_sync/core/fixtures
    yt/yt_sync/core/test_lib
    yt/yt_sync/scenario/helpers
)

END()
