PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PY_SRCS(
    NAMESPACE yt_odin.logserver

    __init__.py
)

PEERDIR(
    yt/odin/lib/yt_odin/logging
    yt/odin/lib/yt_odin/storage
)

END()
