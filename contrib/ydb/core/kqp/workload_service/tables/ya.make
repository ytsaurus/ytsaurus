LIBRARY()

SRCS(
    table_queries.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/workload_service/common

    contrib/ydb/library/query_actor

    contrib/ydb/library/table_creator
)

YQL_LAST_ABI_VERSION()

END()
