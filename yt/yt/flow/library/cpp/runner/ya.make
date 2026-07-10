LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    admin_http_handlers.cpp
    admin_service.cpp
    config.cpp
    debug_build_warning.cpp
    endpoint_provider.cpp
    init.cpp
    node.cpp
    node_info.cpp
    queue_log_writer.cpp
    simple_runner_program.cpp
    vanilla_launcher.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/vanilla
    yt/yt/flow/library/cpp/worker
    yt/yt/flow/library/cpp/client
    yt/yt/flow/library/cpp/controller
    yt/yt/flow/library/cpp/companion
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/misc
    yt/yt/flow/lib/native_client
    yt/yt/client/cache
    yt/yt/library/auth
    yt/yt/library/coredumper
    yt/yt/library/monitoring
    yt/yt/library/orchid
    yt/yt/library/profiling/solomon
    yt/yt/library/program
    yt/yt/library/tcmalloc
    yt/yt/library/tracing/jaeger
    library/cpp/colorizer
    library/cpp/yt/mlock
    library/cpp/yt/phdr_cache
    library/cpp/yt/string/enable_enum_suggestions_on_enum_parse_error
)

END()

RECURSE_FOR_TESTS(
    unittests
)
