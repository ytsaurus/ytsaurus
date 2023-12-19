PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

PEERDIR(yt/go/proto/core/misc)

PROTO_ADDINCL(
    GLOBAL
    yt
)

SRCS(
    ${ARCADIA_ROOT}/yt/yt_proto/yt/core/tracing/proto/span.proto
    ${ARCADIA_ROOT}/yt/yt_proto/yt/core/tracing/proto/tracing_ext.proto
)

END()
