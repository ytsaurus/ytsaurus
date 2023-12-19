LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    action_manager.cpp
    bootstrap.cpp
    bundle_state.cpp
    config.cpp
    dynamic_config_manager.cpp
    helpers.cpp
    program.cpp
    table_registry.cpp
    tablet_action.cpp
    tablet_balancer.cpp
)

PEERDIR(
    library/cpp/yt/phdr_cache

    library/cpp/getopt

    yt/yt/library/dynamic_config

    yt/yt/ytlib
    yt/yt/core

    yt/yt/server/lib/admin
    yt/yt/server/lib/cypress_election
    yt/yt/server/lib/cypress_registrar
    yt/yt/server/lib/misc
    yt/yt/server/lib/tablet_balancer
    yt/yt/server/lib/tablet_server
)

END()

IF (NOT OPENSOURCE)
    RECURSE(
        bin
    )
ENDIF()
