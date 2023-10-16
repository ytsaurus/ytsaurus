LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    server.cpp
)

PEERDIR(
    yt/yt/core
)

END()
