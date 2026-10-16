PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    easy_mode.py
    yt_sync_mini.py
)

# Aliases of the yt_sync modules flow tests and examples import.
IF (OPENSOURCE)
    PY_SRCS(
        NAMESPACE yt.yt_sync
        runner.py
    )

    PY_SRCS(
        NAMESPACE yt.yt_sync.core
        constants.py
    )
ENDIF()

PEERDIR(
    yt/python/yt/wrapper
    yt/python/yt/yson
    yt/yt/flow/library/python/pipeline_tables
)

END()

RECURSE_FOR_TESTS(
    tests
)
