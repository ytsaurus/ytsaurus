PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

PEERDIR(
    yt/go/proto/core/yson
)

SRCS(
    ${ARCADIA_ROOT}/yt/yt_proto/yt/client/cache/proto/config.proto
)

END()
