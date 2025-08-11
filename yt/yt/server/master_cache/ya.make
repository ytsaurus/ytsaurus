LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    bootstrap.cpp
    part_bootstrap_detail.cpp
    chaos_cache_part_bootstrap.cpp
    config.cpp
    dynamic_config_manager.cpp
    master_cache_part_bootstrap.cpp
    program.cpp
)

PEERDIR(
    library/cpp/getopt

    library/cpp/yt/phdr_cache

    yt/yt/library/dynamic_config
    yt/yt/library/server_program

    yt/yt/ytlib

    yt/yt/server/lib
    yt/yt/server/lib/chaos_cache
    yt/yt/server/lib/chaos_node
    yt/yt/server/lib/cypress_registrar
)

END()
