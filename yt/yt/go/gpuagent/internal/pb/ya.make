PROTO_LIBRARY()

GRPC()

ONLY_TAGS(GO_PROTO)

SRCS(${ARCADIA_ROOT}/yt/yt/go/gpuagent/api/api.proto)

END()
