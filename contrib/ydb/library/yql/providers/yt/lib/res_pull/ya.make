LIBRARY()

SRCS(
    res_or_pull.cpp
    table_limiter.cpp
)

PEERDIR(
    library/cpp/yson
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/yt/codec
    contrib/ydb/library/yql/providers/yt/lib/mkql_helpers
)

YQL_LAST_ABI_VERSION()

END()
