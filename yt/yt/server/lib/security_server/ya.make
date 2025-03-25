LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    helpers.cpp
    permission_checker.cpp

    proto/security_manager.proto
)

PEERDIR(
    yt/yt/client
)

END()
