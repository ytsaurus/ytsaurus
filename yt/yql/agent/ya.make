RECURSE(
    bin
)

RECURSE_FOR_TESTS(
    unittests
)

LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    bootstrap.cpp
    config.cpp
    dynamic_config_manager.cpp
    yql_agent.cpp
    yql_service.cpp
    interop.cpp
)

PEERDIR(
    library/cpp/yt/phdr_cache

    library/cpp/getopt

    library/cpp/yt/logging/backends/arcadia

    yt/yt/library/dynamic_config
    yt/yt/library/skiff_ext

    yt/yt/ytlib

    yt/yt/client/formats

    yt/yt/server/lib/admin
    yt/yt/server/lib/cypress_election
    yt/yt/server/lib/misc

    yt/yql/plugin
)

IF (YQL_NATIVE)
    PEERDIR(yt/yql/plugin/native)
ELSE()
    PEERDIR(
        yt/yt/library/query/engine
        yt/yt/library/query/row_comparer

        yt/yql/plugin/bridge
    )
ENDIF()

END()
