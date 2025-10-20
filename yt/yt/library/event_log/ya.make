LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    event_log.cpp
)

PEERDIR(
    yt/yt/client

    yt/yt/core
)

END()
