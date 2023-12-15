LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    config.cpp
    performance_counters.cpp
    table_settings.cpp

    proto/tablet_manager.proto
)

PEERDIR(
    yt/yt/library/dynamic_config
    yt/yt/ytlib
    yt/yt/server/lib/hive
    yt/yt/server/lib/election
    yt/yt/server/lib/transaction_supervisor
)

END()
