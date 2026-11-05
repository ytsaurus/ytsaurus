PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    base.py
    yt_sync.py
)

PEERDIR(
    yt/yt/flow/library/python/integration_test_base
)

IF (OPENSOURCE)
    PEERDIR(
        yt/yt/flow/library/python/yt_sync_mini
    )
ELSE()
    PEERDIR(
        yt/yt_sync/runner
    )
ENDIF()

END()
