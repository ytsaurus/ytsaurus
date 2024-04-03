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
    ${ARCADIA_ROOT}/yt/yt_proto/yt/client/tablet_client/proto/lock_mask.proto
)

END()
