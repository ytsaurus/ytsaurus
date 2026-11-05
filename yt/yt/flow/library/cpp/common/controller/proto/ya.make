PROTO_LIBRARY()

PROTO_NAMESPACE(yt)

SRCS(
    worker_tracker_service.proto
)

PEERDIR(
    yt/yt_proto/yt/core
    yt/yt/flow/library/cpp/misc/proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
