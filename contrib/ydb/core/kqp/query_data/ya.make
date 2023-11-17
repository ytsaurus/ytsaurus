LIBRARY()

SRCS(
    kqp_query_data.cpp
    kqp_prepared_query.cpp
    kqp_predictor.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/kqp/common/simple
    contrib/ydb/library/yql/dq/expr_nodes
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/core/kqp/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()