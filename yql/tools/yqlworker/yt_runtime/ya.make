LIBRARY()

IF (OS_WINDOWS)
    SRCS(
        yt_runtime_win.cpp
    )
ELSE()
    SRCS(
        yt_runtime.cpp
    )

    PEERDIR(
        yt/yt/core
        yt/yt/core/service_discovery/yp
        yt/yt/library/profiling/solomon
    )
ENDIF()

END()
