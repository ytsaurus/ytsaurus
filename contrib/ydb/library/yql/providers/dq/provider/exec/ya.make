LIBRARY()

SRCS(
    yql_dq_exectransformer.cpp
    yql_dq_exectransformer.h
)

PEERDIR(
    library/cpp/yson/node
    library/cpp/svnversion
    library/cpp/digest/md5
    library/cpp/threading/future
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/src/client/driver
    yql/essentials/core
    yql/essentials/core/dq_integration
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/dq/tasks
    contrib/ydb/library/yql/dq/type_ann
    yql/essentials/providers/common/gateway
    yql/essentials/providers/common/metrics
    yql/essentials/providers/common/schema/expr
    yql/essentials/providers/common/transform
    contrib/ydb/library/yql/providers/dq/actors
    contrib/ydb/library/yql/providers/dq/api/grpc
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/counters
    contrib/ydb/library/yql/providers/dq/expr_nodes
    contrib/ydb/library/yql/providers/dq/opt
    contrib/ydb/library/yql/providers/dq/planner
    contrib/ydb/library/yql/providers/dq/runtime
    yql/essentials/providers/result/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()
