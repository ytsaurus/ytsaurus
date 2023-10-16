PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

PEERDIR(
    yt/go/proto/core/ytree
)

SRCS(
    ${ARCADIA_ROOT}/yt/yt_proto/yt/client/discovery_client/proto/discovery_client_service.proto
)

END()
