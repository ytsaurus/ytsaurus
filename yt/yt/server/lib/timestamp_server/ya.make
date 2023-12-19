LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    config.cpp
    timestamp_manager.cpp

    proto/timestamp_manager.proto
)

PEERDIR(
    yt/yt/ytlib
)

END()
