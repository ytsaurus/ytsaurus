LIBRARY()

SRCS(
    kqp_gateway.cpp
    kqp_ic_gateway.cpp
    kqp_metadata_loader.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/provider
    contrib/ydb/core/kqp/query_data
    contrib/ydb/core/kqp/gateway/actors
    contrib/ydb/core/kqp/gateway/behaviour/tablestore
    contrib/ydb/core/kqp/gateway/behaviour/table
    contrib/ydb/core/kqp/gateway/behaviour/external_data_source
    contrib/ydb/library/yql/providers/result/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()
