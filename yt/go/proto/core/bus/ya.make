PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

PEERDIR(
    yt/go/proto/core/misc
)

PROTO_NAMESPACE(yt)

SRCS(${ARCADIA_ROOT}/yt/yt_proto/yt/core/bus/proto/bus.proto)

END()
