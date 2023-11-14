LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    bootstrap.cpp
    config.cpp
    dynamic_config_manager.cpp
    program.cpp
    replicated_table_tracker_host.cpp
)

PEERDIR(
    yt/yt/server/lib/admin
    yt/yt/server/lib/cypress_election
    yt/yt/server/lib/cypress_registrar
    yt/yt/server/lib/hydra
    yt/yt/server/lib/misc
    yt/yt/server/lib/tablet_server

    yt/yt/library/dynamic_config

    yt/yt/ytlib
    yt/yt/core

    library/cpp/yt/memory
    library/cpp/yt/phdr_cache
    library/cpp/getopt
)

END()

IF (NOT OPENSOURCE)
    RECURSE(
        bin
    )
ENDIF()
