PY3_PROGRAM(yt_sync)

STYLE_PYTHON()

PY_SRCS(
    __main__.py
    pipelines.py
    stages.py
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
