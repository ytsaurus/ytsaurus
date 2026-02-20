LIBRARY()

SRCS(
    kqp_rbo_transformer.cpp
    kqp_operator.cpp
    kqp_expression.cpp
    kqp_stage_graph.cpp
    kqp_rbo_utils.cpp
    kqp_rbo.cpp
    kqp_rbo_rules.cpp
    kqp_plan_conversion_utils.cpp
    kqp_rbo_type_ann.cpp
    kqp_rename_unused_stage.cpp
    kqp_constant_folding_stage.cpp
    kqp_rewrite_select.cpp
    kqp_rbo_compute_statistics.cpp
    kqp_rbo_statistics.cpp
    kqp_rbo_dp_cost_based.cpp
    kqp_prune_columns_stage.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/opt/logical
    contrib/ydb/core/kqp/opt/peephole
    contrib/ydb/core/kqp/opt/physical
    contrib/ydb/core/kqp/opt/rbo/physical_convertion
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/opt
    contrib/ydb/library/yql/dq/type_ann
    contrib/ydb/library/yql/providers/s3/expr_nodes
    contrib/ydb/library/yql/providers/s3/statistics
    contrib/ydb/library/yql/utils/plan
    contrib/ydb/core/kqp/provider
    contrib/ydb/library/formats/arrow/protos
)

YQL_LAST_ABI_VERSION()

END()
