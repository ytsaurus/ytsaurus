LIBRARY()

SRCS(
    yql_dq_exectransformer.cpp
    yql_dq_exectransformer.h
)

PEERDIR(
    library/cpp/digest/md5
    library/cpp/svnversion
    library/cpp/threading/future
    library/cpp/yson/node
    contrib/ydb/library/yql/dq/expr_nodes
    contrib/ydb/library/yql/dq/opt
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/dq/tasks
    contrib/ydb/library/yql/dq/type_ann
    contrib/ydb/library/yql/providers/dq/actors
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/counters
    contrib/ydb/library/yql/providers/dq/expr_nodes
    contrib/ydb/library/yql/providers/dq/opt
    contrib/ydb/library/yql/providers/dq/planner
    contrib/ydb/library/yql/providers/dq/provider
    yql/essentials/core
    yql/essentials/core/dq_integration
    yql/essentials/core/peephole_opt
    yql/essentials/core/services
    yql/essentials/core/type_ann
    yql/essentials/minikql
    yql/essentials/minikql/runtime_settings
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/schema/expr
    yql/essentials/providers/common/transform
    yql/essentials/providers/result/expr_nodes
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()
