UNITTEST_FOR(yt/yql/providers/dq/local_gateway)

SRCS(
    yql_dq_gateway_local_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/dq/comp_nodes/llvm16
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/dq/transform
    yql/essentials/minikql
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/computation/llvm16
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/providers/common/comp_nodes
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
