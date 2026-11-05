PY3_LIBRARY()

IF (OPENSOURCE_PROJECT != "yt-cpp-sdk")
    INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

    PEERDIR(
        yt/python/yt/wrapper
        yt/python/yt/environment
    )

    PY_SRCS(
        NAMESPACE yt.environment.components.query_tracker

        __init__.py
    )
ENDIF()

END()
