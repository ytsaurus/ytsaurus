PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

PEERDIR(yt/go/proto/core/ytree)

PROTO_NAMESPACE(yt)

SRCS(
    convert.go
    ${ARCADIA_ROOT}/yt/yt_proto/yt/core/misc/proto/error.proto
    ${ARCADIA_ROOT}/yt/yt_proto/yt/core/misc/proto/guid.proto
    ${ARCADIA_ROOT}/yt/yt_proto/yt/core/misc/proto/hyperloglog.proto
)

END()
