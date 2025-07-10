LIBRARY()

SRCS(
    actors_factory.cpp
    coordinator.cpp
    leader_election.cpp
    probes.cpp
    row_dispatcher.cpp
    row_dispatcher_service.cpp
    topic_session.cpp
)

PEERDIR(
    contrib/ydb/core/fq/libs/actors/logging
    contrib/ydb/core/fq/libs/config/protos
    contrib/ydb/core/fq/libs/metrics
    contrib/ydb/core/fq/libs/row_dispatcher/events
    contrib/ydb/core/fq/libs/row_dispatcher/format_handler
    contrib/ydb/core/fq/libs/row_dispatcher/purecalc_compilation
    contrib/ydb/core/fq/libs/shared_resources
    contrib/ydb/core/fq/libs/ydb
    contrib/ydb/core/mon

    contrib/ydb/library/actors/core
    contrib/ydb/library/security
    contrib/ydb/library/yql/dq/actors
    contrib/ydb/library/yql/dq/actors/common
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/providers/pq/provider

    contrib/ydb/public/sdk/cpp/adapters/issue
    contrib/ydb/public/sdk/cpp/src/client/scheme
    contrib/ydb/public/sdk/cpp/src/client/table
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    purecalc_no_pg_wrapper
    format_handler
)

IF(NOT EXPORT_CMAKE)
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
