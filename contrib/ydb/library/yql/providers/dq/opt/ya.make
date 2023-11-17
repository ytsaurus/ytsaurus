LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/providers/dq/expr_nodes
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/transform
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/dq/opt
    contrib/ydb/library/yql/dq/type_ann
    contrib/ydb/library/yql/dq/integration
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/peephole_opt
    contrib/ydb/library/yql/core/type_ann
    contrib/ydb/library/yql/minikql/computation/llvm
    library/cpp/yson/node
)

SRCS(
    dqs_opt.cpp
    logical_optimize.cpp
    physical_optimize.cpp
)

YQL_LAST_ABI_VERSION()

END()
