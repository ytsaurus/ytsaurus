LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

INCLUDE(../../RpcProxyProtocolVersion.txt)
INCLUDE(../../ya_check_dependencies.inc)

PROTO_NAMESPACE(yt)

SRCS(
    proto/admin_service.proto
    proto/restart_service.proto
)

PEERDIR(
    contrib/libs/protobuf
    yt/yt/core
)

END()
