PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    base.py
    filters.py
)

PEERDIR(
    yt/yt_sync/core/model
)

END()

RECURSE_FOR_TESTS(
    ut
)
