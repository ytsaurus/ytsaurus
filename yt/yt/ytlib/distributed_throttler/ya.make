LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

INCLUDE(../../RpcProxyProtocolVersion.txt)
INCLUDE(../../ya_check_dependencies.inc)

PROTO_NAMESPACE(yt)

SRCS(
    proto/distributed_throttler.proto

    config.cpp
    distributed_throttler.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/library/discovery_client
)

END()
