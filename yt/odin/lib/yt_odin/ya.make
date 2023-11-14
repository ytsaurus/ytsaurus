PY23_LIBRARY()

PY_SRCS(
    NAMESPACE yt_odin

    __init__.py
)

END()

RECURSE(
    logging
    logserver
    odinserver
    storage
    test_helpers
)
