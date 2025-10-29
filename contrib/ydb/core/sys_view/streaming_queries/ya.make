LIBRARY()

SRCS(
    streaming_queries.cpp
)

PEERDIR(
    library/cpp/protobuf/json
    contrib/ydb/core/kqp/common/events
    contrib/ydb/core/kqp/gateway/behaviour/streaming_query/common
    contrib/ydb/core/kqp/runtime
    contrib/ydb/core/kqp/workload_service/actors
    contrib/ydb/core/sys_view/common
    contrib/ydb/library/actors/core
    contrib/ydb/library/query_actor
)

YQL_LAST_ABI_VERSION()

END()
