LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    bootstrap.cpp
    zookeeper_manager.cpp
    zookeeper_shard.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/server/lib/hydra
)

END()
