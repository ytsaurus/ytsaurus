LIBRARY()

SRCS(
    kqp_rbo_transformer.cpp
    kqp_operator.cpp
    kqp_rbo.cpp
    kqp_rbo_rules.cpp
    kqp_convert_to_physical.cpp
    kqp_plan_conversion_utils.cpp
    kqp_rbo_type_ann.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/opt/logical
    contrib/ydb/core/kqp/opt/peephole
    contrib/ydb/core/kqp/opt/physical
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
