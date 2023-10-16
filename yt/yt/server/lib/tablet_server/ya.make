LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    proto/backup_manager.proto
    proto/tablet_manager.proto

    config.cpp
    performance_counters.cpp
    replicated_table_tracker.cpp
)

PEERDIR(
    yt/yt/ytlib
    yt/yt/server/lib/tablet_node
)

END()


RECURSE_FOR_TESTS(
    unittests
)
