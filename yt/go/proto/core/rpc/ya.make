PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

PEERDIR(
    yt/go/proto/core/misc
    yt/go/proto/core/tracing
)

PROTO_ADDINCL(
    GLOBAL
    yt
)

SRCS(${ARCADIA_ROOT}/yt/yt_proto/yt/core/rpc/proto/rpc.proto)

END()

RECURSE(unittests)
