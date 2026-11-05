LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    proto/token_service.proto
)

PEERDIR(
    yt/yt/core
)

END()
