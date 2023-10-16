LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    public.cpp
    private.cpp
    bootstrap.cpp
    coordinator.cpp
    context.cpp
    access_checker.cpp
    api.cpp
    dynamic_config_manager.cpp
    http_authenticator.cpp
    formats.cpp
    helpers.cpp
    compression.cpp
    framing.cpp
    config.cpp
    zookeeper_bootstrap_proxy.cpp

    clickhouse/discovery_cache.cpp
    clickhouse/config.cpp
    clickhouse/handler.cpp
    clickhouse/public.cpp
)

IF (OPENSOURCE)
    SRCS(compression_opensource.cpp)

    PEERDIR(
        library/cpp/blockcodecs/core
    )
ELSE()
    SRCS(compression_yandex.cpp)

    PEERDIR(
        library/cpp/streams/lzop
        library/cpp/streams/lz
        library/cpp/blockcodecs
    )
ENDIF()

PEERDIR(
    yt/yt/client/driver
    yt/yt/ytlib
    yt/yt/library/auth_server
    yt/yt/library/clickhouse_discovery
    yt/yt/library/dynamic_config
    yt/yt/core/https
    library/cpp/yt/phdr_cache
    yt/yt/server/lib
    yt/yt/server/lib/chunk_pools
    yt/yt/server/lib/cypress_registrar
    yt/yt/server/lib/zookeeper_proxy
    yt/yt/server/lib/logging
    library/cpp/streams/brotli
    library/cpp/string_utils/base64
    library/cpp/getopt
    library/cpp/cgiparam
)

END()

IF (NOT OPENSOURCE)
    RECURSE(
        bin
    )
ENDIF()

RECURSE_FOR_TESTS(
    unittests
)
