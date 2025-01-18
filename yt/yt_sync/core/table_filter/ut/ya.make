PY3TEST()

STYLE_PYTHON()

TEST_SRCS(
    test_table_filter.py
)

PEERDIR(
    yt/yt_sync/core/fixtures
    yt/yt_sync/core/model
    yt/yt_sync/core/table_filter

    yt/python/client
)

END()
