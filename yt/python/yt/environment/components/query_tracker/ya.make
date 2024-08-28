PY3_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/python/yt/wrapper
    yt/python/yt/environment
)

PY_SRCS(
    NAMESPACE yt.environment.components.query_tracker

    __init__.py
)

END()
