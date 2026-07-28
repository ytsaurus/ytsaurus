PY3_PROGRAM()

PY_SRCS(
    __main__.py
    common.py
    pipelines.py
    queues.py
    stages.py
    tables.py
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
