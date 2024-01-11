LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    access_checker.cpp
    api.cpp
    bootstrap.cpp
    config.cpp
    context.cpp
    coordinator.cpp
    dynamic_config_manager.cpp
    formats.cpp
    framing.cpp
    helpers.cpp
    http_authenticator.cpp
    private.cpp
    profilers.cpp
    public.cpp
    zookeeper_bootstrap_proxy.cpp

    clickhouse/config.cpp
    clickhouse/discovery_cache.cpp
    clickhouse/handler.cpp
    clickhouse/public.cpp
)

PEERDIR(
    yt/yt/client/driver
    yt/yt/ytlib
    yt/yt/library/auth_server
    yt/yt/library/clickhouse_discovery
    yt/yt/library/dynamic_config
    yt/yt/library/ytprof
    yt/yt/library/containers
    yt/yt/library/containers/disk_manager
    yt/yt/core/https
    yt/yt/server/lib
    yt/yt/server/lib/chunk_pools
    yt/yt/server/lib/cypress_registrar
    yt/yt/server/lib/zookeeper_proxy
    yt/yt/server/lib/logging
    library/cpp/cgiparam
    library/cpp/getopt
    library/cpp/streams/brotli
    library/cpp/string_utils/base64
    library/cpp/yt/phdr_cache
)

END()

RECURSE_FOR_TESTS(
    unittests
)
