PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
)

PEERDIR(
    yt/yt_sync/action
    yt/yt_sync/core
    yt/yt_sync/sync
    yt/yt_sync/scenario

    yt/python/client
)

END()

RECURSE(
    action
    core
    docs
    example
    lock
    runner
    scenario
    sync
)

IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(
        ft
        ft_chaos
    )
ENDIF()
