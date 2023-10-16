PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

PROTO_ADDINCL(
    GLOBAL
    yt
)

SRCS(${ARCADIA_ROOT}/yt/yt_proto/yt/core/yson/proto/protobuf_interop.proto)

END()
