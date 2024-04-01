PY3_LIBRARY()

PY_SRCS(
    NAMESPACE yt_odin

    __init__.py
)

END()

RECURSE(
    common
    logging
    logserver
    odinserver
    storage
    test_helpers
)
