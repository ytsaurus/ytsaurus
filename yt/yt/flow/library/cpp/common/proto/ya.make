PROTO_LIBRARY()

PROTO_NAMESPACE(yt)

# Package the python modules under the protoc-visible (namespace-stripped)
# path, so generated cross-file imports resolve; same convention as yt_proto.
PY_NAMESPACE(yt.flow.library.cpp.common.proto)

SRCS(
    admin_service.proto
    message.proto
    timer.proto
    visit.proto
)

PEERDIR(
    yt/yt_proto/yt/core
)

EXCLUDE_TAGS(GO_PROTO)

END()
