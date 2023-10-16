LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    alert_manager.cpp
    bootstrap.cpp
    config.cpp
    consumer_controller.cpp
    cypress_synchronizer.cpp
    dynamic_config_manager.cpp
    helpers.cpp
    performance_counters.cpp
    profile_manager.cpp
    queue_agent.cpp
    queue_agent_sharding_manager.cpp
    queue_controller.cpp
    snapshot_representation.cpp
    queue_static_table_exporter.cpp
)

PEERDIR(
    library/cpp/yt/phdr_cache

    library/cpp/getopt

    yt/yt/library/dynamic_config

    yt/yt/client
    yt/yt/client/federated

    yt/yt/ytlib

    yt/yt/server/lib/admin
    yt/yt/server/lib/cypress_election
    yt/yt/server/lib/cypress_registrar
    yt/yt/server/lib/misc
)

END()

IF (NOT OPENSOURCE)
    RECURSE(
        bin
    )
ENDIF()
