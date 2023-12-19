LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    proto/table_manager.proto
)

PEERDIR(
    yt/yt/client
    yt/yt/server/lib/tablet_server
)

END()
