PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    base.py
    chaos.py
    replicated.py
)

PEERDIR(
    yt/yt_sync/core/client
    yt/yt_sync/core/model
)

END()
