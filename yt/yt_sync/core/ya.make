PY3_LIBRARY()

STYLE_PYTHON()


PY_SRCS(
    __init__.py
    client.py
    helpers.py
    settings.py
    state_builder.py
    table_filter.py
    yt_commands.py
)

PEERDIR(
    library/python/confmerge
    yt/python/client_with_rpc
    yt/yt_sync/core/constants
    yt/yt_sync/core/model
    yt/yt_sync/core/spec
)

END()

RECURSE(
    constants
    diff
    model
    spec
)

RECURSE_FOR_TESTS(
    fixtures
    test_lib
    ut
)

IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(
        ft
    )
ENDIF()
