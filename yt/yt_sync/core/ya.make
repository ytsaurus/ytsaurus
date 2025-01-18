PY3_LIBRARY()

STYLE_PYTHON()


PY_SRCS(
    __init__.py
)

PEERDIR(
    yt/yt_sync/core/client
    yt/yt_sync/core/constants
    yt/yt_sync/core/model
    yt/yt_sync/core/settings
    yt/yt_sync/core/spec
    yt/yt_sync/core/state_builder
    yt/yt_sync/core/table_filter
    yt/yt_sync/core/yt_commands
)

END()

RECURSE(
    client
    constants
    diff
    helpers
    model
    settings
    spec
    spec_merger
    state_builder
    table_filter
    yt_commands
)

RECURSE_FOR_TESTS(
    fixtures
    test_lib
)
