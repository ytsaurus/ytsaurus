LIBRARY()

SRCS(
    kqp_check_script_lease_actor.cpp
    kqp_finalize_script_actor.cpp
    kqp_finalize_script_service.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/counters
    contrib/ydb/core/kqp/federated_query/actors
    contrib/ydb/core/kqp/proxy_service
    contrib/ydb/core/mind
    contrib/ydb/core/tx/scheme_cache
    contrib/ydb/library/table_creator
    contrib/ydb/library/yql/providers/s3/actors_factory
)

YQL_LAST_ABI_VERSION()

END()
