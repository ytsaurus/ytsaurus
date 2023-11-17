LIBRARY()

SRCS(
    mkql_helpers.cpp
)

PEERDIR(
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/utils
)

YQL_LAST_ABI_VERSION()

END()
