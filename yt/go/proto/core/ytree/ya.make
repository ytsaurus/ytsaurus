PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

PEERDIR(yt/go/proto/core/yson)

PROTO_ADDINCL(
    GLOBAL
    yt
)

SRCS(
    ${ARCADIA_ROOT}/yt/yt_proto/yt/core/ytree/proto/attributes.proto
    ${ARCADIA_ROOT}/yt/yt_proto/yt/core/ytree/proto/request_complexity_limits.proto
)

END()
