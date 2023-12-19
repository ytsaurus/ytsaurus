LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    proto/discovery_server_service.proto

    config.cpp
    cypress_integration.cpp
    discovery_server.cpp
    group.cpp
    group_manager.cpp
    group_tree.cpp
    helpers.cpp
    member.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/ytlib
    yt/yt/ytlib/discovery_client
)

END()

RECURSE_FOR_TESTS(
    unittests
)
