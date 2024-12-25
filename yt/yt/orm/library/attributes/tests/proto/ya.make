PROTO_LIBRARY()

SRCS(
    scalar_attribute.proto
    wire_string.proto
)

PEERDIR(
    yt/yt_proto/yt/core
)

ONLY_TAGS(CPP_PROTO)

END()
