PROTO_LIBRARY()

PROTO_NAMESPACE(yt)

SRCS(
    companion_service.proto
)

PEERDIR(
    yt/yt_proto/yt/core
    yt/yt/flow/library/cpp/common/proto
)

EXCLUDE_TAGS(GO_PROTO)

IF (OPENSOURCE_PROJECT != "yt-cpp-sdk")
    GRPC()
ENDIF()

END()
