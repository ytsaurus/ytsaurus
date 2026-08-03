PROTO_LIBRARY()

GRPC()

ONLY_TAGS(GO_PROTO)

PROTO_NAMESPACE(yt)

PEERDIR(
    yt/go/proto/core/misc
    yt/go/proto/flow/common
)

SRCS(
    ${ARCADIA_ROOT}/yt/yt/flow/library/cpp/companion/proto/companion_service.proto
)

END()
