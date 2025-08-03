LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    helpers.cpp
    permission_checker.cpp

    proto/security_manager.proto

    user_access_validator.cpp
    config.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/server/lib/misc
)

END()
