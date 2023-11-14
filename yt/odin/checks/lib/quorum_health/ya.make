PY3_LIBRARY()

PY_SRCS(
    NAMESPACE yt_odin_checks.lib.quorum_health

    __init__.py
)

PEERDIR(
    yt/python/yt/wrapper
    contrib/python/requests
)

END()
