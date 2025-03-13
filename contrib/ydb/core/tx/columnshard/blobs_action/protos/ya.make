PROTO_LIBRARY()

SRCS(
    events.proto
    blobs.proto
)

PEERDIR(
    contrib/ydb/library/actors/protos
)

END()
