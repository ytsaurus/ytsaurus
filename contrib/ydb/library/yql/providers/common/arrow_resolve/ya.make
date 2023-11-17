LIBRARY()

SRCS(
    yql_simple_arrow_resolver.cpp
)

PEERDIR(
    contrib/ydb/library/yql/minikql/arrow
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/providers/common/mkql
)

YQL_LAST_ABI_VERSION()

END()
