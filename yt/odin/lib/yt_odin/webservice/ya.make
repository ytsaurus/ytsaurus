PY3_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/odin/lib/yt_odin/logserver
)

PY_SRCS(
    NAMESPACE yt_odin.webservice

    __init__.py
    solomon.py
)

END()
