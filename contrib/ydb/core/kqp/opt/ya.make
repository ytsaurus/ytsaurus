LIBRARY()

SRCS(
    kqp_opt.cpp
    kqp_opt_build_phy_query.cpp
    kqp_opt_build_txs.cpp
    kqp_opt_effects.cpp
    kqp_opt_kql.cpp
    kqp_opt_phase.cpp
    kqp_opt_phy_check.cpp
    kqp_opt_phy_finalize.cpp
    kqp_query_blocks_transformer.cpp
    kqp_query_plan.cpp
    kqp_statistics_transformer.cpp
    kqp_column_statistics_requester.cpp
    kqp_constant_folding_transformer.cpp
    kqp_opt_hash_func_propagate_transformer.cpp
    kqp_type_ann.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/opt/logical
    contrib/ydb/core/kqp/opt/peephole
    contrib/ydb/core/kqp/opt/physical
    contrib/ydb/core/kqp/opt/rbo
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

GENERATE_ENUM_SERIALIZATION(kqp_query_plan.h)

END()
