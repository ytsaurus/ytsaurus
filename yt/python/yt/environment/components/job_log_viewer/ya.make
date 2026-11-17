PY3_LIBRARY()

IF (OPENSOURCE_PROJECT != "yt-cpp-sdk")
    INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

    PEERDIR(
        yt/python/yt/environment
        yt/python/contrib/python-requests
    )

    PY_SRCS(
        NAMESPACE yt.environment.components.job_log_viewer

        __init__.py
    )
ENDIF()

END()
