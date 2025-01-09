LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    access_checker.cpp
    bootstrap.cpp
    config.cpp
    discovery_service.cpp
    dynamic_config_manager.cpp
    bundle_dynamic_config_manager.cpp
    program.cpp
    query_corpus_reporter.cpp
)

PEERDIR(
    yt/yt/server/lib
    yt/yt/server/lib/chunk_pools
    yt/yt/server/lib/rpc_proxy
    yt/yt/server/lib/cypress_registrar
    yt/yt/server/lib/shuffle_server

    yt/yt/ytlib

    yt/yt/client/arrow

    yt/yt/core/rpc/grpc

    yt/yt/library/auth_server
    yt/yt/library/dynamic_config
    yt/yt/library/server_program
    yt/yt/library/monitoring
    yt/yt/library/profiling/solomon
    yt/yt/library/tracing/jaeger

    library/cpp/yt/phdr_cache

    library/cpp/getopt
)

END()

RECURSE_FOR_TESTS(
    unittests
)
