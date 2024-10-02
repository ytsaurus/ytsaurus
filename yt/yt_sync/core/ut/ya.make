PY3TEST()

STYLE_PYTHON()

TEST_SRCS(
    test_helpers.py
    test_settings.py
    test_state_builder.py
    test_table_filter.py
)

PEERDIR(
    yt/yt_sync/core
    yt/yt_sync/core/fixtures
    yt/yt_sync/core/test_lib
    yt/python/client
)

END()
