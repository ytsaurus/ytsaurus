LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    config.cpp
    structs.cpp

    proto/job_profile.proto
)

PEERDIR(
    library/cpp/protobuf/interop

    yt/yt/ytlib
)

END()
