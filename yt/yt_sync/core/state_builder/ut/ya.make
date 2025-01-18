PY3TEST()

STYLE_PYTHON()

TEST_SRCS(
    base.py
    test_actual.py
    test_desired.py
    test_desired_deprecated.py
)

PEERDIR(
    yt/yt_sync/core/constants
    yt/yt_sync/core/fixtures
    yt/yt_sync/core/model
    yt/yt_sync/core/settings
    yt/yt_sync/core/spec
    yt/yt_sync/core/state_builder
    yt/yt_sync/core/table_filter
    yt/yt_sync/core/test_lib
)

END()
