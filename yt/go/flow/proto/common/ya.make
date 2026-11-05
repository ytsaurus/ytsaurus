PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

PROTO_NAMESPACE(yt)

PEERDIR(
    yt/go/proto/core/misc
)

SRCS(
    ${ARCADIA_ROOT}/yt/yt/flow/library/cpp/common/proto/message.proto
    ${ARCADIA_ROOT}/yt/yt/flow/library/cpp/common/proto/timer.proto
    ${ARCADIA_ROOT}/yt/yt/flow/library/cpp/common/proto/visit.proto
)

END()
