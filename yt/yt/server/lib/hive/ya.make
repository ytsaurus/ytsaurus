LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    avenue_directory.cpp
    config.cpp
    helpers.cpp
    hive_manager.cpp
    hive_manager_v1.cpp
    hive_manager_v2.cpp
    persistent_mailbox_state_cookie.cpp
    logical_time_registry.cpp
    mailbox_v1.cpp
    mailbox_v2.cpp
    public.cpp

    proto/hive_manager.proto
)

PEERDIR(
    yt/yt/core
    yt/yt/server/lib/election
    yt/yt/server/lib/hydra
)

END()
