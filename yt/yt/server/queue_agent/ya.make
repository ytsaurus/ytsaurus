LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    bootstrap.cpp
    config.cpp
    consumer_controller.cpp
    cypress_synchronizer.cpp
    dynamic_config_manager.cpp
    helpers.cpp
    performance_counters.cpp
    profile_manager.cpp
    program.cpp
    queue_agent.cpp
    queue_agent_sharding_manager.cpp
    queue_controller.cpp
    snapshot_representation.cpp
    queue_exporter.cpp
    queue_export_manager.cpp
)

PEERDIR(
    library/cpp/yt/phdr_cache

    library/cpp/getopt

    yt/yt/library/dynamic_config
    yt/yt/library/program
    yt/yt/library/server_program

    yt/yt/client
    yt/yt/client/federated

    yt/yt/ytlib

    yt/yt/server/lib/admin
    yt/yt/server/lib/alert_manager
    yt/yt/server/lib/cypress_election
    yt/yt/server/lib/cypress_registrar
    yt/yt/server/lib/misc
)

END()
