LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    bootstrap.cpp
    chaos_cache.cpp
    chaos_cache_bootstrap.cpp
    chaos_cache_service.cpp
    config.cpp
    dynamic_config_manager.cpp
    master_cache_bootstrap.cpp
    program.cpp
)

PEERDIR(
    library/cpp/getopt

    library/cpp/yt/phdr_cache

    yt/yt/library/dynamic_config

    yt/yt/ytlib

    yt/yt/server/lib
    yt/yt/server/lib/chaos_cache
    yt/yt/server/lib/cypress_registrar
)

END()
