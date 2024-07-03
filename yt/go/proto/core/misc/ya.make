PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

PEERDIR(yt/go/proto/core/ytree)

PROTO_ADDINCL(
    GLOBAL
    yt
)

SRCS(
    convert.go
    ${ARCADIA_ROOT}/yt/yt_proto/yt/core/misc/proto/guid.proto
    ${ARCADIA_ROOT}/yt/yt_proto/yt/core/misc/proto/error.proto
)

END()
