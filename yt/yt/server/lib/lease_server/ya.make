LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

OWNER(g:yt)

PROTO_NAMESPACE(yt)

SRCS(
    config.cpp
    lease_manager.cpp

    proto/lease_manager.proto
)

PEERDIR(
    yt/yt/core

    yt/yt/server/lib/hive
    yt/yt/server/lib/hydra
)

END()
