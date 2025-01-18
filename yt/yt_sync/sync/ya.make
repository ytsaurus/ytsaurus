PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
)

PEERDIR(
    yt/yt_sync/core
    yt/yt_sync/lock
    yt/yt_sync/scenario
)

END()
