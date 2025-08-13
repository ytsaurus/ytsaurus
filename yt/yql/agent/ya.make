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
    type_builder.cpp
    data_builder.cpp
)

PEERDIR(
    library/cpp/yt/phdr_cache

    library/cpp/getopt

    library/cpp/yt/logging/backends/arcadia
    library/cpp/yt/mlock

    yt/yt/library/dynamic_config
    yt/yt/library/skiff_ext
    yt/yt/library/server_program
    yt/yt/library/profiling/perf

    yt/yt/ytlib

    yt/yt/client/formats

    yt/yt/server/lib/admin
    yt/yt/server/lib/cypress_election
    yt/yt/server/lib/misc
    yt/yt/server/lib/component_state_checker

    yt/yql/plugin

    yt/yt/library/query/engine
    yt/yt/library/query/row_comparer
    yt/yt/library/monitoring

    yt/yql/plugin/bridge

    yql/essentials/public/langver
    yql/essentials/public/result_format
)

END()
