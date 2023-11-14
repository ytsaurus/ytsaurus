PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/odin/lib/yt_odin/logging
    yt/odin/lib/yt_odin/logserver
    yt/python/yt/wrapper
    contrib/python/python-dateutil
)
    
IF (NOT OPENSOURCE)
    PEERDIR(
        contrib/python/python-prctl
    )
ENDIF()

PY_SRCS(
    NAMESPACE yt_odin.odinserver

    __init__.py
    odin.py
    alerts.py
    common.py
    check_discovery.py
    check_task.py
    juggler_client.py
)

END()
