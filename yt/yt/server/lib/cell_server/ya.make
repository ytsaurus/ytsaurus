LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    proto/cell_manager.proto
)

PEERDIR(
    yt/yt_proto/yt/core
)

END()
