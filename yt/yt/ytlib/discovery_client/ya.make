LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

INCLUDE(../../ya_check_dependencies.inc)

PROTO_NAMESPACE(yt)

SRCS(
    config.cpp
    discovery_client.cpp
    helpers.cpp
    member_client.cpp
    public.cpp
    request_session.cpp
)
PEERDIR(
    contrib/libs/protobuf
    yt/yt/core
    yt/yt/client
)

END()
