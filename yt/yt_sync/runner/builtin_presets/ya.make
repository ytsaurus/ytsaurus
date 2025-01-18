PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    pipeline.py
)

PEERDIR(
    yt/yt_sync/core/constants
)

END()
