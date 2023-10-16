LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    config.cpp
    proto/user_job_synchronizer_service.proto
)

PEERDIR(
    yt/yt_proto/yt/core
)

END()
