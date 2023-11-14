PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PY_SRCS(
    NAMESPACE yt_odin_checks.lib.check_runner
    __init__.py
    argument_manager.py
)

PEERDIR(
    yt/odin/lib/yt_odin/logging
    yt/odin/lib/yt_odin/logserver
    yt/python/client
)

IF (NOT OPENSOURCE)
    PEERDIR(
        contrib/python/python-prctl
    )
ENDIF()

END()
