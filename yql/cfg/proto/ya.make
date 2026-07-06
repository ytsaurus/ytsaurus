PROTO_LIBRARY()

SRCS(
    api_config.proto
    worker_config.proto
    grpc_config.proto
    yqltornado_config.proto
)

PEERDIR(
    yql/essentials/protos
    yql/essentials/providers/common/proto
    yql/essentials/utils/log/proto
    yt/yql/providers/dq/config
    yt/yql/providers/yt/lib/access_provider/proto
    yt/yql/providers/yt/lib/tvm_client/proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
