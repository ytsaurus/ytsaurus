LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    config.cpp
    structs.cpp
)

PEERDIR(
    library/cpp/protobuf/interop

    yt/yt/ytlib
    yt/yt/server/lib/job_proxy
    yt/yt/server/lib/misc
)

END()
