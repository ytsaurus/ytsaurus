PROGRAM()

PEERDIR(
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

SRCS(
    alloc.cpp
)

YQL_LAST_ABI_VERSION()

END()
