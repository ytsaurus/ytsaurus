LIBRARY()

SRCS(
    yql_eval_expr.cpp
    yql_eval_expr.h
    yql_eval_params.cpp
    yql_eval_params.h
    yql_out_transformers.cpp
    yql_out_transformers.h
    yql_lineage.cpp
    yql_lineage.h
    yql_plan.cpp
    yql_plan.h
    yql_transform_pipeline.cpp
    yql_transform_pipeline.h
)

PEERDIR(
    library/cpp/string_utils/base64
    library/cpp/yson
    contrib/ydb/library/yql/ast/serialize
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/common_opt
    contrib/ydb/library/yql/core/peephole_opt
    contrib/ydb/library/yql/core/type_ann
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/schema/expr
    contrib/ydb/library/yql/providers/result/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    mounts
)
