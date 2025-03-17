LIBRARY()

SRCS(
    yql_s3_statistics.cpp
)

PEERDIR(
    yql/essentials/core
    contrib/ydb/library/yql/providers/dq/expr_nodes
    contrib/ydb/library/yql/providers/s3/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()
