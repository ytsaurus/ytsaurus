LIBRARY()

SRCS(
    kqp_mkql_compiler.cpp
    kqp_olap_compiler.cpp
    kqp_query_compiler.cpp
)

PEERDIR(
    contrib/ydb/core/formats
    contrib/ydb/core/kqp/common
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/library/mkql_proto
    yql/essentials/core/arrow_kernels/request
    yql/essentials/core/dq_integration
    contrib/ydb/library/yql/dq/opt
    contrib/ydb/library/yql/dq/type_ann
    contrib/ydb/library/yql/dq/tasks
    yql/essentials/minikql
    yql/essentials/providers/common/mkql
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/s3/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()
