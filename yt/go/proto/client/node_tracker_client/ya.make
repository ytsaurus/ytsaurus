PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

PEERDIR(
    yt/go/proto/core/misc
)

PROTO_ADDINCL(
    GLOBAL
    yt
)

SRCS(
    # TODO(aleksandr.gaev): Adding this file produces an error `node_directory.pb.go:188:2: undefined: file_yt_proto_yt_client_node_tracker_client_proto_node_proto_init`.
    # ${ARCADIA_ROOT}/yt/yt_proto/yt/client/node_tracker_client/proto/node_directory.proto
    ${ARCADIA_ROOT}/yt/yt_proto/yt/client/node_tracker_client/proto/node.proto
)

END()
