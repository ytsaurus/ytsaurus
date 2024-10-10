LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    avenue_directory.cpp
    config.cpp
    helpers.cpp
    hive_manager.cpp
    hive_manager_v1.cpp
    persistent_mailbox_state.cpp
    logical_time_registry.cpp
    mailbox_v1.cpp
    public.cpp

    proto/hive_manager.proto
)

PEERDIR(
    yt/yt/core
    yt/yt/server/lib/election
)

END()
