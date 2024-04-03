PY3_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PY_SRCS(
    NAMESPACE yt_odin.common

    __init__.py
    prctl.py
)

END()
