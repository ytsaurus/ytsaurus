PROTO_LIBRARY()

SRCS(
    task.proto
)

PEERDIR(
    yql/essentials/public/issue/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
