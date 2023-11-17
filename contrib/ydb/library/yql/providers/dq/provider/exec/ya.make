LIBRARY()

SRCS(
    yql_dq_exectransformer.cpp
    yql_dq_exectransformer.h
)

PEERDIR(
    library/cpp/yson/node
    library/cpp/svnversion
    library/cpp/digest/md5
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/dq/integration
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/dq/tasks
    contrib/ydb/library/yql/dq/type_ann
    contrib/ydb/library/yql/providers/common/gateway
    contrib/ydb/library/yql/providers/common/metrics
    contrib/ydb/library/yql/providers/common/schema/expr
    contrib/ydb/library/yql/providers/common/transform
    contrib/ydb/library/yql/providers/dq/actors
    contrib/ydb/library/yql/providers/dq/api/grpc
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/counters
    contrib/ydb/library/yql/providers/dq/expr_nodes
    contrib/ydb/library/yql/providers/dq/opt
    contrib/ydb/library/yql/providers/dq/planner
    contrib/ydb/library/yql/providers/dq/runtime
    contrib/ydb/library/yql/providers/result/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()
