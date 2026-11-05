LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    access_checker.cpp
    api_service_impl.cpp
    api_service_impl_admin.cpp
    api_service_impl_cypress.cpp
    api_service_impl_distributed_files.cpp
    api_service_impl_distributed_tables.cpp
    api_service_impl_dynamic_tables.cpp
    api_service_impl_file_cache.cpp
    api_service_impl_files.cpp
    api_service_impl_flow.cpp
    api_service_impl_job_info.cpp
    api_service_impl_jobs.cpp
    api_service_impl_journals.cpp
    api_service_impl_operation_info.cpp
    api_service_impl_operations.cpp
    api_service_impl_queries.cpp
    api_service_impl_queues.cpp
    api_service_impl_replicated_tables.cpp
    api_service_impl_security.cpp
    api_service_impl_shuffle.cpp
    api_service_impl_static_tables.cpp
    api_service_impl_transactions.cpp
    config.cpp
    format_row_stream.cpp
    multiconnection_client_cache.cpp
    multiproxy_access_validator.cpp
    profilers.cpp
    helpers.cpp
    proxy_coordinator.cpp
)

PEERDIR(
    yt/yt/ytlib
    yt/yt/library/auth_server

    yt/yt/client
    yt/yt/client/arrow

    # TODO(max42): eliminate.
    yt/yt/server/lib/misc

    yt/yt/library/error_skeleton
    yt/yt/library/tracing/jaeger

    yt/yt/server/lib/transaction_server
    yt/yt/server/lib/security_server

    yt/yt/library/signature/components

    yt/yt/core
)

END()

RECURSE_FOR_TESTS(
    unittests
)
