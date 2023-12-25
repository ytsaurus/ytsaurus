LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    config.cpp
    lease_manager.cpp
    serialize.cpp

    proto/lease_manager.proto
)

PEERDIR(
    yt/yt/core

    yt/yt/server/lib/hive
    yt/yt/server/lib/hydra
)

END()
