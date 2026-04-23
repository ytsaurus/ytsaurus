LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    election_manager.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/client
)

END()
