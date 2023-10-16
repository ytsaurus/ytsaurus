PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

PEERDIR(
    yt/go/proto/core/misc
    yt/go/proto/core/ytree
    yt/go/proto/client/chaos_client
    yt/go/proto/client/chunk_client
    yt/go/proto/client/hive
)

SRCS(
    ${ARCADIA_ROOT}/yt/yt_proto/yt/client/api/rpc_proxy/proto/discovery_service.proto
    ${ARCADIA_ROOT}/yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.proto
)

END()
