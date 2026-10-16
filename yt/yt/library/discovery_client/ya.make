LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

INCLUDE(../../ya_check_dependencies.inc)

PROTO_NAMESPACE(yt)

SRCS(
    config.cpp
    discovery.cpp
    discovery_base.cpp
    discovery_client.cpp
    helpers.cpp
    member_client.cpp
    public.cpp
    request_session.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/yson/node
    library/cpp/yt/threading
    yt/yt/core
    yt/yt/library/profiling
    yt/yt_proto/yt/client
)

END()
