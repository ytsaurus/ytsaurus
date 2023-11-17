PROGRAM()

SRCS(
    parse.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/v1
    contrib/ydb/library/yql/sql/pg_dummy
)

END()
