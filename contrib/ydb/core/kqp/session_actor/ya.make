LIBRARY()

SRCS(
    kqp_log_query.cpp
    kqp_query_state.cpp
    kqp_query_stats.cpp
    kqp_response.cpp
    kqp_session_actor.cpp
    kqp_temp_tables_manager.cpp
    kqp_worker_actor.cpp
    kqp_worker_common.cpp
)

PEERDIR(
    contrib/ydb/core/docapi
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/federated_query
    contrib/ydb/public/sdk/cpp/src/library/operation_id
    contrib/ydb/core/tx/schemeshard
)

YQL_LAST_ABI_VERSION()

END()
