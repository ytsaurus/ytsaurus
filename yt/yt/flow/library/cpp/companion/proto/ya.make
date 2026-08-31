PROTO_LIBRARY()

PROTO_NAMESPACE(yt)

# Package the python modules under the protoc-visible (namespace-stripped)
# path, so generated cross-file imports resolve; same convention as yt_proto.
PY_NAMESPACE(yt.flow.library.cpp.companion.proto)

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
