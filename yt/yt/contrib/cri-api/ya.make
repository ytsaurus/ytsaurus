PROTO_LIBRARY()

LICENSE(Apache-2.0)

PROTO_NAMESPACE(
    yt/yt/contrib/cri-api
)

PEERDIR(
    yt/yt/contrib/gogoproto
)

GRPC()

ONLY_TAGS(
    CPP_PROTO
)

SRCS(
    k8s.io/cri-api/pkg/apis/runtime/v1/api.proto
)

END()
