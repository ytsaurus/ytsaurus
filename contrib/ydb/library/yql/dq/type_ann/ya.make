LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/type_ann
    contrib/ydb/library/yql/dq/expr_nodes
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/providers/common/provider
)

SRCS(
    dq_type_ann.cpp
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(dq_type_ann.h)

END()
