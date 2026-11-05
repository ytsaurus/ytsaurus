PY3_LIBRARY()

PY_SRCS(
    __init__.py
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
